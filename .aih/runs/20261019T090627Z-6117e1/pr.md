## Add a "draining" scheduling state for Dags (issue #22006): an operator stops new scheduled runs

### Plan

The request ships as one PR, so the plan is a single task. This revision adds `airflow-core/src/airflow/models/backfill.py` to the editable files. It needs `Index("idx_backfill_dag_id_completed_at", dag_id, completed_at)` in `Backfill.__table_args__` so the ORM metadata matches migration 0134. I did not verify the paths for the UI datamodels, `common/db/dags.py` or the new migration, so the implementer should confirm them. Regenerating the airflow-ctl and task-sdk clients is not planned, and the React UI is untouched.

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

- `INTENT1` Add enum `DagSchedulingState` in `airflow.utils.state` with exactly the string values `active`, `draining`, `paused`. (change)
- `INTENT2` Add non-null boolean `DagModel.is_draining` (default false, indexed, check constraint `NOT (is_paused AND is_draining)`), read-only `scheduling_state` property and `set_scheduling_state(state)` setting both flags consistently, with a 3.4.0 Alembic migration (also adding index on `backfill (dag_id, completed_at)`) and the migrations reference updated. (change)
- `INTENT3` While a Dag is draining, exclude it from Dags needing scheduled runs, record asset events without queuing the Dag (direct, alias, name/URI), and create no run for its pending partitioned-asset events. (change)
- `INTENT4` For a draining Dag, keep scheduling/queuing task instances of queued and running runs and examining unfinished runs as for an active Dag; explicit triggers (manual, operator, asset materialisation, backfill) remain allowed. (change)
- `INTENT5` Add `SchedulerJobRunner._finalize_draining_dags(session)` run every 5 seconds in the scheduler loop, moving a draining Dag to paused when it has no queued/running run and no accepted-but-uninitialised backfill (backfill row without Dag runs), writing a `Log` entry with event `drain_completed`, owner `scheduler`, processing in `dag_id` order with per-query limit and row locks. (change)
- `INTENT6` Automatic pausing after the configured consecutive failed runs pauses the Dag and clears draining. (change)
- `INTENT7` Add `scheduling_state` (default `active`) to `DAGResponse` and `DAGDetailsResponse`; `PATCH /dags/{dag_id}` and `PATCH /dags` require exactly one non-null of `is_paused`/`scheduling_state` (else 422, explicit null for the unused field accepted), `update_mask` may name one of them and a mismatch with the body is 422, responses reflect the new state, bulk PATCH sets both flags, and `is_paused` clients behave as before (clearing draining). (change)
- `INTENT8` In `/ui/dags` add a `scheduling_state` filter (existing `paused` filter unchanged, `paused=false` includes draining), add `has_unfinished_runs` to every listed Dag independent of `dag_runs_limit`, and exclude draining Dags from dashboard `active_dag_count`. (change)
- `INTENT9` `airflow dags pause`/`unpause` set paused/active and clear draining (a draining Dag counts as needing change), and `dags details` includes `scheduling_state`. (change)
- `INTENT10` Document draining in the Dag concepts page and FAQ and add a newsfragment announcing it. (change)
- `INTENT11` Add or extend unit tests for the model, scheduler, asset manager, public and UI REST routes and CLI, keeping existing suites green. (change)

### Criteria, tests and results

| Criterion | Kind | Task | Test(s) | Result |
|---|---|---|---|---|
| ACC19 | behavior | T2 | `airflow-core/tests/unit/utils/test_state.py::test_dag_scheduling_state_members` (covered, probe verified / base does_not_build) | committed |
| ACC20 | behavior | T2 | `airflow-core/tests/unit/models/test_dag.py::TestDagModel::test_set_scheduling_state` (covered, probe verified / base does_not_build) | committed |
| ACC21 | other | T2 | airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/cli/commands/test_dag_command.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py, airflow-core/tests/unit/utils/test_state.py | committed |
| ACC22 | behavior | T2 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::test_partitioned_dag_run_not_created_for_draining_dag` (covered, probe verified / base does_not_build) | committed |
| ACC23 | behavior | T2 | `airflow-core/tests/unit/assets/test_manager.py::TestAssetManager::test_register_asset_change_skips_draining_dag` (covered, probe verified / base does_not_build) | committed |
| ACC24 | behavior | T2 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::TestSchedulerJob::test_execute_task_instances_draining_dag_still_queues` (covered, probe verified / base does_not_build) | committed |
| ACC25 | behavior | T2 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::test_finalize_draining_dags_pauses_idle_dags` (covered, probe verified / base fails) | committed |
| ACC26 | behavior | T2 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::test_finalize_draining_dags_waits_for_uninitialised_backfill` (covered, probe verified / base fails) | committed |
| ACC27 | behavior | T2 | `airflow-core/tests/unit/jobs/test_scheduler_job.py::test_scheduler_loop_registers_finalize_draining_dags_every_5_seconds` (covered, probe verified / base fails) | committed |
| ACC28 | behavior | T2 | `airflow-core/tests/unit/models/test_dag.py::TestDagModel::test_draining_dag_is_paused_and_not_draining_after_limit` (covered, probe verified / base does_not_build) | committed |
| ACC29 | behavior | T2 | `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py::TestGetDag::test_get_dag_reflects_draining` (covered, probe verified / base does_not_build) | committed |
| ACC30 | behavior | T2 | `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py::TestPatchDag::test_patch_dag_requires_exactly_one_field` (covered, probe verified / base fails) | committed |
| ACC31 | behavior | T2 | `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py::TestPatchDags::test_patch_dags_sets_both_flags` (covered, probe verified / base does_not_build) | committed |
| ACC32 | behavior | T2 | `airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py::TestGetDagRuns::test_scheduling_state_filter` (covered, probe verified / base does_not_build) | committed |
| ACC33 | behavior | T2 | `airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py::TestGetDagRuns::test_has_unfinished_runs_ignores_dag_runs_limit` (covered, probe verified / base does_not_build) | committed |
| ACC34 | behavior | T2 | `airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py::TestDagStatsEndpoint::test_draining_dag_is_not_counted_as_active` (covered, probe verified / base does_not_build) | committed |
| ACC35 | behavior | T2 | `airflow-core/tests/unit/cli/commands/test_dag_command.py::TestCliDags::test_pause_unpause_change_draining_dag` (covered, probe verified / base does_not_build) | committed |
| ACC36 | other | T2 | airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py, airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py, airflow-core/tests/unit/assets/test_manager.py, airflow-core/tests/unit/cli/commands/test_dag_command.py, airflow-core/tests/unit/jobs/test_scheduler_job.py, airflow-core/tests/unit/models/test_dag.py, airflow-core/tests/unit/models/test_dagrun.py, airflow-core/tests/unit/utils/test_state.py | committed |

### Tasks

- `T2` Single PR: add DagSchedulingState enum, then DagModel column/property/setter, Backfill index in __table_args__ and migration 0134 (db.py head map, migrations-ref); then scheduler changes (dags_needing_dagruns, partitioned-asset runs, _finalize_draining_dags + 5s timer), asset manager, dagrun auto-pause; then public and UI API (datamodels, PATCH validation, filter, has_unfinished_runs, dashboard, openapi yamls), CLI; then docs, newsfragment; extend/adjust existing tests alongside each layer. Do not touch the React UI. (1 attempt(s), commit `40f612f29d`)
  - reviewer note [pass 4] `airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py:155`: The active-count `func.coalesce(func.sum(case((and_(...)))))` line is far over the line length; ruff format will rewrap it.
  - reviewer note [pass 2] `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py:966`: The new test methods are preceded by an extra blank line inside the class (ruff format will fix it).

### Gate failures along the way

- change.implemented (T1, attempt 1): change.replan_requested

### Cost (client-side estimate)

- sessions: 8; turns: 384; USD: 6.47; attempts: 2

_Generated by aih run `20261019T090627Z-6117e1`._
