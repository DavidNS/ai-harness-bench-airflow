You are implementing one task of an approved plan inside an isolated worktree. You can read and
edit files and you have a shell: run the project's tests the way its developers do (keep the
output short, e.g. `| tail`), its formatter and linters, its code generators, `git diff` to review
your change. Destructive commands, pushing and the network are denied by a hook; do not work
around it. A scratch test to try a hypothesis is fine if named with `aih_probe` in the file name and deleted before you
finish (those are the only files you may create outside the task's list). When you try to finish,
the harness runs the task's tests and gives you the result; the whole suite runs once after the
last task. Edits outside the files listed for this task are rejected by a hook; if the task really
needs another file, say so in your final answer (`replan_request` with the files) and the harness
will add it to your task.

## Original request

Add a "draining" scheduling state for Dags (issue #22006): an operator stops new scheduled runs
while the work already in flight finishes, and the Dag ends up paused on its own.

Pausing a Dag today is abrupt. Running task instances finish, but queued and downstream tasks of
the current runs are stranded until the Dag is unpaused, and nothing says "stop starting new runs,
let the current ones complete". That bites during upgrades and maintenance windows; the workaround
in the wild is to set every schedule to None, wait, do the work, and put the schedules back. The
design agreed on the issue: a third mode next to paused and unpaused, "drain, then pause", that is
transient and switches to paused by itself when the drain is complete. Pause and unpause stay
available from the API and the CLI as today, and existing clients that only know `is_paused` must
keep working unchanged.

Behaviour to deliver:

- States. A Dag's scheduling state is `active`, `draining` or `paused` (enum `DagSchedulingState`
  in `airflow.utils.state`, with exactly those string values). `draining` is transient and always
  converges to `paused`; the resting states remain `active` and `paused`. A Dag is never paused and
  draining at the same time.
- Model and migration. `DagModel` gets a non-null boolean column `is_draining` (default false,
  indexed, with the check constraint `NOT (is_paused AND is_draining)`), a read-only property
  `scheduling_state` derived from the two flags, and `set_scheduling_state(state)` that sets both
  flags consistently. The Alembic migration belongs to 3.4.0 (migrations reference updated) and
  also adds an index on `backfill (dag_id, completed_at)`, which the scheduler needs to find a
  backfill that is accepted but not yet initialised. `is_paused` keeps its meaning everywhere.
- Scheduler, while a Dag is draining. No new scheduler-created runs: the Dag is excluded from the
  Dags needing a run (scheduled runs), an asset event is still recorded but does not queue the Dag
  (whether it is scheduled on the asset directly, through an alias, or by asset name or URI), and
  no run is created for pending partitioned-asset events of the Dag. Task instances of its queued
  and running runs keep being scheduled and queued exactly as for an active Dag (unlike a paused
  Dag), and its unfinished runs keep being examined. Explicit triggers (manual, operator-triggered,
  asset materialisation, backfill) remain available while draining, as they are for a paused Dag,
  and simply extend the drain until the resulting work finishes.
- Finalisation. The scheduler runs a step `_finalize_draining_dags(session)` on
  `SchedulerJobRunner` every 5 seconds in its loop. It moves a draining Dag to `paused` once the
  Dag has no unfinished run (queued or running) and no backfill that is accepted but not yet
  initialised (a backfill row without associated Dag runs: the backfill is committed before its
  runs exist), and it records an audit `Log` entry with event `drain_completed` for that Dag, owner
  `scheduler`. Dags are processed in `dag_id` order with the usual per-query limit and row locks.
- Automatic pausing after the configured number of consecutive failed runs pauses the Dag and
  clears draining.
- Public REST API. `DAGResponse` and `DAGDetailsResponse` gain `scheduling_state` (default
  `active`). `PATCH /dags/{dag_id}` and `PATCH /dags` accept either `is_paused` (boolean) or
  `scheduling_state`: exactly one of the two must be non-null, otherwise 422 (both present, both
  null, or the only field sent as null). Generated clients send the field they are not using as an
  explicit null, which is accepted. `update_mask` may name `is_paused` or `scheduling_state`, one
  of them; a mask that names the other field than the one the body carries is a 422. The response
  reflects the new state; the bulk PATCH sets both flags for every matched Dag. Clients that send
  `is_paused` behave as before (true pauses, false activates, and both clear draining).
- UI API (`/ui/dags`). A `scheduling_state` filter (`active`, `draining`, `paused`); the existing
  `paused` filter keeps its meaning (`paused=false` includes draining Dags). Every Dag in the list
  carries `has_unfinished_runs`, true when any queued or running run exists, regardless of
  `dag_runs_limit` (which only limits the latest runs shown). The dashboard's `active_dag_count`
  does not count draining Dags.
- CLI. `airflow dags pause` and `airflow dags unpause` set `paused` and `active` respectively and
  clear draining; a draining Dag counts as a Dag the command has to change. `dags details`
  includes `scheduling_state`.
- Documentation: the Dag concepts page and the FAQ describe draining; a newsfragment announces it.

Out of scope: the React UI under `airflow-core/src/airflow/ui` is handled in a separate change; do
not touch it. Do not change unrelated behaviour.

Add or extend the unit tests for the model, the scheduler, the asset manager, the public and UI
REST routes and the CLI. The existing test suites must stay green.

## Plan summary

Single PR adding the draining scheduling state: enum, DagModel column/property/setter, migration (plus backfill index), scheduler exclusions and 5-second finalisation, auto-pause clearing, public/UI API and CLI changes, docs and a newsfragment. The React UI is untouched. Generated artefacts (OpenAPI spec, migrations reference, ERD, airflow-ctl datamodels, DB head map) need regenerating via their normal hooks.

## Your task: T1 (draining-state)

Order: (1) add DagSchedulingState enum; (2) add DagModel column/property/setter, Backfill index in model, migration, migrations-ref/head map/ERD; (3) scheduler/asset manager/dagrun behaviour and finalisation; (4) API datamodels, public and UI routes, parameters, dashboard; (5) CLI; (6) update existing tests asserting old response shapes and add new tests; (7) docs and newsfragment. Use set_scheduling_state in API/CLI paths; no new AirflowException; session-taking functions must not commit.

Acceptance criteria this task must satisfy:
- ACC1 [behavior]: DagSchedulingState has exactly the members ACTIVE='active', DRAINING='draining', PAUSED='paused'.
- ACC2 [behavior]: DagModel.is_draining defaults to False; scheduling_state returns paused/draining/active from the flags; set_scheduling_state sets both flags consistently; the database rejects is_paused and is_draining both true.
- ACC3 [other]: New 3.4.0 migration revises f8c2a1d94e03, adds non-null is_draining (server default false) with index and check constraint, and index on backfill(dag_id, completed_at); migrations-ref.rst, DB head map and ERD are updated; models match the migration.
- ACC4 [behavior]: dags_needing_dagruns excludes draining Dags; an asset event for a draining Dag (direct, alias, name or URI reference) is recorded but creates no AssetDagRunQueue row; pending partitioned-asset events of a draining Dag produce no Dag run.
- ACC5 [behavior]: Task instances of queued/running runs of a draining Dag are still scheduled and queued, its unfinished runs are still examined, and a manual trigger or backfill can still create runs.
- ACC6 [behavior]: _finalize_draining_dags moves a draining Dag with no queued/running run and no backfill lacking Dag runs to paused and writes a Log with event drain_completed and owner scheduler; it leaves Dags with an unfinished run or an uninitialised backfill draining; Dags are processed in dag_id order with limit and row locks; the step is registered every 5 seconds in the scheduler loop.
- ACC7 [behavior]: Auto-pause after max consecutive failed runs sets is_paused True and is_draining False for a draining Dag.
- ACC8 [behavior]: DAGResponse and DAGDetailsResponse expose scheduling_state, default active, reflecting the model.
- ACC9 [behavior]: PATCH /dags/{dag_id} and PATCH /dags accept exactly one non-null of is_paused or scheduling_state (explicit null for the other accepted); both present, both null, or only null returns 422; update_mask naming is_paused or scheduling_state must match the body field else 422; response and bulk update set both flags for every matched Dag; is_paused true pauses and false activates, both clearing draining.
- ACC10 [behavior]: /ui/dags supports scheduling_state filter (active, draining, paused); paused=false still includes draining Dags; every Dag has has_unfinished_runs true iff a queued or running run exists regardless of dag_runs_limit; dashboard active_dag_count excludes draining Dags.
- ACC11 [behavior]: dags pause sets paused and dags unpause sets active, both clearing draining; a draining Dag is matched as needing change by both commands; dags details output includes scheduling_state.
- ACC12 [other]: core-concepts/dags.rst and faq.rst describe draining; an airflow-core newsfragment announces it (written 'Dag'); no UI files changed.
- ACC13 [other]: Tests added in model, scheduler, asset manager, public API, UI API, dashboard and CLI suites; existing tests asserting exact response shapes are updated.

Files you may edit (anything else is blocked):
- airflow-core/src/airflow/utils/state.py (production, modify)
- airflow-core/src/airflow/models/dag.py (production, modify)
- airflow-core/src/airflow/models/backfill.py (production, modify)
- airflow-core/src/airflow/models/dagrun.py (production, modify)
- airflow-core/src/airflow/migrations/versions/0134_3_4_0_add_dag_is_draining.py (production, create)
- airflow-core/src/airflow/utils/db.py (production, modify)
- airflow-core/docs/migrations-ref.rst (production, modify)
- airflow-core/docs/img/airflow_erd.sha256 (production, modify)
- airflow-core/docs/img/airflow_erd.svg (production, modify)
- airflow-core/src/airflow/jobs/scheduler_job_runner.py (production, modify)
- airflow-core/src/airflow/assets/manager.py (production, modify)
- airflow-core/src/airflow/api_fastapi/common/parameters.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py (production, modify)
- airflow-core/src/airflow/api_fastapi/logging/decorators.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml (production, modify)
- airflow-ctl/src/airflowctl/api/datamodels/generated.py (production, modify)
- airflow-core/src/airflow/cli/commands/dag_command.py (production, modify)
- airflow-core/docs/core-concepts/dags.rst (production, modify)
- airflow-core/docs/faq.rst (production, modify)
- airflow-core/newsfragments/22006.significant.rst (production, create)
- airflow-core/tests/unit/models/test_dag.py (test, modify)
- airflow-core/tests/unit/models/test_dagrun.py (test, modify)
- airflow-core/tests/unit/jobs/test_scheduler_job.py (test, modify)
- airflow-core/tests/unit/assets/test_manager.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py (test, modify)
- airflow-core/tests/unit/cli/commands/test_dag_command.py (test, modify)

Tests that must pass when you finish: airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/cli/commands/test_dag_command.py

## Rules

- Write or update the tests listed above so that they fail without your production change and
  pass with it. The harness verifies this against the base commit: a test that also passes
  without the change does not count.
- Every branch you add (a new `if`/`else`, an `except`, an early return, a new error status)
  must be exercised by a test in this task's test files before you finish. An independent
  reviewer blocks the task for each new branch without a test; write them first.
- Do not modify existing tests other than the ones listed for this task.
- Do not add features, docs or refactors the task does not ask for.
- If the task cannot be done without touching a file that is not listed, or a criterion is
  impossible as written, do not improvise: stop and return `replan_request` with the reason and the
  files you need. That re-plans instead of failing you.

## Final answer

Return JSON matching the schema you were given: a short `summary`, one `criteria_notes` entry per
criterion with the test that proves it, and `replan_request` (`null` unless you need a re-plan).
