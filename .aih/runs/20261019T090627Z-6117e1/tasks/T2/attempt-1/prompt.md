You are implementing one task of an approved plan inside an isolated worktree. You can read and
edit files. The only command you can run is `aih check`, as often as you need; every other shell
command is rejected. `aih check <test files...>` runs those tests; `aih check --build` only
compiles (when the project has a build step); `--race` runs them under the race detector and
`--run <pattern>` restricts them to matching tests, when the project supports it; and
`aih check --probe <file>` runs a scratch test file you wrote and deletes it afterwards, so you can
try a hypothesis without it becoming part of the change (name such files `aih_probe*`; they are
the only files you may create outside the task's list). When you try to finish, the harness runs the
task's tests again and gives you the result. Edits outside the files listed for this task are
rejected by a hook; do not work around it.

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

The request ships as one PR, so the plan is a single task. This revision adds `airflow-core/src/airflow/models/backfill.py` to the editable files. It needs `Index("idx_backfill_dag_id_completed_at", dag_id, completed_at)` in `Backfill.__table_args__` so the ORM metadata matches migration 0134. I did not verify the paths for the UI datamodels, `common/db/dags.py` or the new migration, so the implementer should confirm them. Regenerating the airflow-ctl and task-sdk clients is not planned, and the React UI is untouched.

## Your task: T2 (T1)

Single PR: add DagSchedulingState enum, then DagModel column/property/setter, Backfill index in __table_args__ and migration 0134 (db.py head map, migrations-ref); then scheduler changes (dags_needing_dagruns, partitioned-asset runs, _finalize_draining_dags + 5s timer), asset manager, dagrun auto-pause; then public and UI API (datamodels, PATCH validation, filter, has_unfinished_runs, dashboard, openapi yamls), CLI; then docs, newsfragment; extend/adjust existing tests alongside each layer. Do not touch the React UI.

Acceptance criteria this task must satisfy:
- ACC19 [behavior]: DagSchedulingState has exactly members/values active, draining, paused (test_state.py).
- ACC20 [behavior]: DagModel.scheduling_state derives active/draining/paused from flags; set_scheduling_state sets both flags consistently; is_draining defaults to false; the DB rejects is_paused and is_draining both true.
- ACC21 [other]: Migration 0134 (3.4.0) adds is_draining column with index and check constraint plus index idx_backfill_dag_id_completed_at on backfill (dag_id, completed_at); Backfill.__table_args__ declares the same index; utils/db.py head map and migrations-ref.rst updated.
- ACC22 [behavior]: A draining Dag is not returned by dags_needing_dagruns for scheduled runs, and no run is created for its pending partitioned-asset events.
- ACC23 [behavior]: AssetManager records the asset event but does not queue a draining Dag scheduled directly, via alias, or by name/URI; active Dags still queue.
- ACC24 [behavior]: Task instances of queued/running runs of a draining Dag are still scheduled and queued, and its unfinished runs are still examined; manual triggers still create runs.
- ACC25 [behavior]: _finalize_draining_dags moves a draining Dag with no queued/running run and no backfill without Dag runs to paused, clears draining and logs drain_completed with owner scheduler.
- ACC26 [behavior]: _finalize_draining_dags leaves a Dag draining when it has a queued or running run, or a backfill row with no Dag runs; processes in dag_id order with limit and row locks.
- ACC27 [behavior]: The scheduler loop registers _finalize_draining_dags with a 5 second interval.
- ACC28 [behavior]: Auto-pause after consecutive failed runs sets is_paused true and is_draining false for a draining Dag.
- ACC29 [behavior]: DAGResponse/DAGDetailsResponse expose scheduling_state defaulting to active; GET reflects draining.
- ACC30 [behavior]: PATCH /dags/{dag_id} and PATCH /dags accept exactly one non-null of is_paused/scheduling_state (explicit null for the other accepted); both, neither, or a null-only field yield 422.
- ACC31 [behavior]: update_mask naming is_paused or scheduling_state works when matching the body; a mask naming the other field is 422; response reflects new state; bulk PATCH sets both flags; is_paused true/false clears draining.
- ACC32 [behavior]: /ui/dags scheduling_state filter returns only matching Dags; paused=false includes draining; paused filter otherwise unchanged.
- ACC33 [behavior]: Every Dag in /ui/dags has has_unfinished_runs true iff a queued/running run exists, regardless of dag_runs_limit.
- ACC34 [behavior]: Dashboard active_dag_count excludes draining Dags.
- ACC35 [behavior]: airflow dags pause sets paused and clears draining; unpause sets active and clears draining; a draining Dag counts as changed by both; dags details output includes scheduling_state.
- ACC36 [other]: dags.rst and faq.rst describe draining; newsfragment airflow-core/newsfragments/22006.feature.rst added; 'Dag' casing used in prose.

Files you may edit (anything else is blocked):
- airflow-core/src/airflow/utils/state.py (production, modify)
- airflow-core/src/airflow/models/dag.py (production, modify)
- airflow-core/src/airflow/models/backfill.py (production, modify)
- airflow-core/src/airflow/models/dagrun.py (production, modify)
- airflow-core/src/airflow/migrations/versions/0134_3_4_0_add_dag_is_draining.py (production, create)
- airflow-core/src/airflow/utils/db.py (production, modify)
- airflow-core/docs/migrations-ref.rst (production, modify)
- airflow-core/src/airflow/jobs/scheduler_job_runner.py (production, modify)
- airflow-core/src/airflow/assets/manager.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py (production, modify)
- airflow-core/src/airflow/api_fastapi/common/parameters.py (production, modify)
- airflow-core/src/airflow/api_fastapi/common/db/dags.py (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml (production, modify)
- airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml (production, modify)
- airflow-core/src/airflow/cli/commands/dag_command.py (production, modify)
- airflow-core/docs/core-concepts/dags.rst (production, modify)
- airflow-core/docs/faq.rst (production, modify)
- airflow-core/newsfragments/22006.feature.rst (production, create)
- airflow-core/tests/unit/utils/test_state.py (test, modify)
- airflow-core/tests/unit/models/test_dag.py (test, modify)
- airflow-core/tests/unit/models/test_dagrun.py (test, modify)
- airflow-core/tests/unit/jobs/test_scheduler_job.py (test, modify)
- airflow-core/tests/unit/assets/test_manager.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py (test, modify)
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py (test, modify)
- airflow-core/tests/unit/cli/commands/test_dag_command.py (test, modify)

Tests that must pass when you finish: airflow-core/tests/unit/utils/test_state.py, airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/cli/commands/test_dag_command.py

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
