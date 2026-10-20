## Add a "draining" scheduling state for Dags (issue #22006): an operator stops new scheduled runs

### Plan

Single PR adding the draining scheduling state: enum, DagModel column/property/setter, migration (plus backfill index), scheduler exclusions and 5-second finalisation, auto-pause clearing, public/UI API and CLI changes, docs and a newsfragment. The React UI is untouched. Generated artefacts (OpenAPI spec, migrations reference, ERD, airflow-ctl datamodels, DB head map) need regenerating via their normal hooks.

### Request

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

### Intents

- `INTENT1` Add enum `DagSchedulingState` in `airflow.utils.state` with exactly the string values `active`, `draining`, `paused`; `draining` is transient and always converges to `paused`, and a Dag is never paused and draining at once. (change)
- `INTENT2` Add non-null boolean `DagModel.is_draining` (default false, indexed, check constraint `NOT (is_paused AND is_draining)`), a read-only `scheduling_state` property, and `set_scheduling_state(state)` that sets both flags consistently. (change)
- `INTENT3` Add an Alembic migration for 3.4.0 (migrations reference updated) that adds `is_draining` and an index on `backfill (dag_id, completed_at)`. (change)
- `INTENT4` While a Dag is draining, exclude it from the Dags needing a scheduled run, record asset events without queuing the Dag (direct, alias, name or URI), and create no run for its pending partitioned-asset events. (change)
- `INTENT5` Task instances of a draining Dag's queued and running runs keep being scheduled and queued as for an active Dag, its unfinished runs keep being examined, and explicit triggers (manual, operator, asset materialisation, backfill) stay available and extend the drain. (already_satisfied)
- `INTENT6` Add `SchedulerJobRunner._finalize_draining_dags(session)` run every 5 seconds in the scheduler loop. It moves a draining Dag to paused when it has no queued or running run and no accepted-but-uninitialised backfill (a backfill row without Dag runs). It writes a `Log` entry with event `drain_completed` and owner `scheduler`, and processes Dags in `dag_id` order with the usual per-query limit and row locks. (change)
- `INTENT7` Automatic pausing after the configured number of consecutive failed runs pauses the Dag and clears draining. (change)
- `INTENT8` `DAGResponse` and `DAGDetailsResponse` gain `scheduling_state` (default `active`). `PATCH /dags/{dag_id}` and `PATCH /dags` accept exactly one non-null of `is_paused` or `scheduling_state`, otherwise 422 (an explicit null for the unused field is accepted). `update_mask` may name either field but must match the body field, otherwise 422. The response reflects the new state and the bulk PATCH sets both flags for every matched Dag. `is_paused` clients behave as before: true pauses, false activates, and both clear draining. (change)
- `INTENT9` `/ui/dags` gains a `scheduling_state` filter (`active`, `draining`, `paused`) while the `paused` filter keeps its meaning (`paused=false` includes draining Dags). Every listed Dag carries `has_unfinished_runs` (any queued or running run, regardless of `dag_runs_limit`). The dashboard `active_dag_count` excludes draining Dags. (change)
- `INTENT10` `airflow dags pause` and `airflow dags unpause` set `paused` and `active` and clear draining, treating a draining Dag as one the command must change. `dags details` includes `scheduling_state`. (change)
- `INTENT11` Describe draining in the Dag concepts page and the FAQ, and add a newsfragment announcing it. (change)
- `INTENT12` Add or extend unit tests for the model, scheduler, asset manager, public and UI REST routes and the CLI, keeping existing suites green. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC1 | behavior | T1 | `airflow-core/tests/unit/models/test_dag.py::TestDagSchedulingStateModel::test_enum_values` (covered, probe verified / base does_not_build) | committed |
| ACC2 | behavior | T1 | `airflow-core/tests/unit/models/test_dag.py::TestDagSchedulingStateModel::test_database_rejects_paused_and_draining` (covered, probe verified / base does_not_build) | committed |
| ACC3 | other | T1 | airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/cli/commands/test_dag_command.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py | committed |
| ACC4 | behavior | T1 | `airflow-core/tests/unit/assets/test_manager.py::TestAssetManager::test_register_asset_change_skips_draining_dag` (covered, probe verified / base fails) | committed |
| ACC5 | behavior | T1 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::TestSchedulerJob::test_enqueue_task_instances_follows_scheduling_state` (covered, probe verified / base fails) | committed |
| ACC6 | behavior | T1 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::TestSchedulerJob::test_finalize_draining_dags` (covered, probe verified / base fails) | committed |
| ACC7 | behavior | T1 | `airflow-core/tests/unit/models/test_dag.py::TestDag::test_draining_dag_is_paused_after_limit` (covered, probe verified / base does_not_build) | committed |
| ACC8 | behavior | T1 | `airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py::TestSchedulingState::test_response_defaults_to_active` (covered, probe verified / base fails) | committed |
| ACC9 | behavior | T1 | `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py::TestPatchDag::test_patch_dag_scheduling_state_invalid_body` (covered, probe verified / base fails) | committed |
| ACC10 | behavior | T1 | `airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py::TestGetDagRuns::test_has_unfinished_runs_ignores_dag_runs_limit` (covered, probe verified / base fails) | committed |
| ACC11 | behavior | T1 | `airflow-core/tests/unit/cli/commands/test_dag_command.py::TestCliDags::test_pause_unpause_draining_dag` (covered, probe verified / base fails) | committed |
| ACC12 | other | T1 | airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/cli/commands/test_dag_command.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py | committed |
| ACC13 | other | T1 | airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/cli/commands/test_dag_command.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py | committed |

### Tasks

- `T1` Order: (1) add DagSchedulingState enum; (2) add DagModel column/property/setter, Backfill index in model, migration, migrations-ref/head map/ERD; (3) scheduler/asset manager/dagrun behaviour and finalisation; (4) API datamodels, public and UI routes, parameters, dashboard; (5) CLI; (6) update existing tests asserting old response shapes and add new tests; (7) docs and newsfragment. Use set_scheduling_state in API/CLI paths; no new AirflowException; session-taking functions must not commit. (1 attempt(s), commit `cf398f395b`)
  - reviewer note [pass 1] `airflow-core/docs/img/airflow_erd.sha256:0`: The ERD artefacts are not in the diff (the declared paths do not exist; the real ones are docs/apache-airflow/img/airflow_erd.*), so the ERD for is_draining and the backfill index is probably not regenerated.
  - reviewer note [pass 2] `airflow-core/tests/unit/jobs/test_scheduler_job.py:1351`: No test shows that the unfinished runs of a draining Dag are still examined (_start_queued_dagruns or _schedule_dag_run); this holds by construction because is_paused stays false, so it is low risk.

### Gate failures along the way

- plan (plan, attempt 1): plan.evidence_quote_not_found

### Cost (client-side estimate)

- sessions: 6; turns: 168; USD: 3.66; attempts: 1

_Generated by aih run `20261020T081745Z-237b12`._
