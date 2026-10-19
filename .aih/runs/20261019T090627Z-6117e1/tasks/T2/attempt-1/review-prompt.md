You are reviewing one task of an approved plan, implemented by a different session whose
reasoning you have not seen. The change is already applied in this worktree. You can read the
repository, and you can run tests through `aih check`, the only command available:
`aih check <test files...>` runs those tests, `--race` under the race detector and
`--run <pattern>` only matching tests (when the project supports them), and
`aih check --probe <file>` runs a scratch test you wrote as a NEW file named `aih_probe*` and
deletes it afterwards. You cannot edit the change: every other file is locked for you. The harness
has already verified mechanically that the diff stays within the task's files, that the changed
tests fail without the production change and pass with it, and that the whole suite is green. Do
not re-check those. Your job is judgement, backed by tests you run.

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

## Task T2 (T1)

Single PR: add DagSchedulingState enum, then DagModel column/property/setter, Backfill index in __table_args__ and migration 0134 (db.py head map, migrations-ref); then scheduler changes (dags_needing_dagruns, partitioned-asset runs, _finalize_draining_dags + 5s timer), asset manager, dagrun auto-pause; then public and UI API (datamodels, PATCH validation, filter, has_unfinished_runs, dashboard, openapi yamls), CLI; then docs, newsfragment; extend/adjust existing tests alongside each layer. Do not touch the React UI.

Acceptance criteria:
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

Files declared for the task:
- airflow-core/src/airflow/utils/state.py (production)
- airflow-core/src/airflow/models/dag.py (production)
- airflow-core/src/airflow/models/backfill.py (production)
- airflow-core/src/airflow/models/dagrun.py (production)
- airflow-core/src/airflow/migrations/versions/0134_3_4_0_add_dag_is_draining.py (production)
- airflow-core/src/airflow/utils/db.py (production)
- airflow-core/docs/migrations-ref.rst (production)
- airflow-core/src/airflow/jobs/scheduler_job_runner.py (production)
- airflow-core/src/airflow/assets/manager.py (production)
- airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py (production)
- airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py (production)
- airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py (production)
- airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py (production)
- airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py (production)
- airflow-core/src/airflow/api_fastapi/common/parameters.py (production)
- airflow-core/src/airflow/api_fastapi/common/db/dags.py (production)
- airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml (production)
- airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml (production)
- airflow-core/src/airflow/cli/commands/dag_command.py (production)
- airflow-core/docs/core-concepts/dags.rst (production)
- airflow-core/docs/faq.rst (production)
- airflow-core/newsfragments/22006.feature.rst (production)
- airflow-core/tests/unit/utils/test_state.py (test)
- airflow-core/tests/unit/models/test_dag.py (test)
- airflow-core/tests/unit/models/test_dagrun.py (test)
- airflow-core/tests/unit/jobs/test_scheduler_job.py (test)
- airflow-core/tests/unit/assets/test_manager.py (test)
- airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py (test)
- airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py (test)
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py (test)
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py (test)
- airflow-core/tests/unit/cli/commands/test_dag_command.py (test)

Test files of this task that the change modified (existing tests, declared by the plan):
- airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py
- airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py
- airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py
- airflow-core/tests/unit/assets/test_manager.py
- airflow-core/tests/unit/cli/commands/test_dag_command.py
- airflow-core/tests/unit/jobs/test_scheduler_job.py
- airflow-core/tests/unit/models/test_dag.py
- airflow-core/tests/unit/models/test_dagrun.py
- airflow-core/tests/unit/utils/test_state.py

Test results: selected tests green; red proven (the full suite runs once, after the last task)

## The diff

```diff
diff --git a/airflow-core/docs/core-concepts/dags.rst b/airflow-core/docs/core-concepts/dags.rst
index ab397f3532..2b14e18650 100644
--- a/airflow-core/docs/core-concepts/dags.rst
+++ b/airflow-core/docs/core-concepts/dags.rst
@@ -811,6 +811,34 @@ downstream tasks are put in to a state of "Scheduled". When the Dag is unpaused,
 begin running according to the Dag logic. Dags with no "scheduled" tasks will begin running according to
 their schedule.
 
+.. _concepts:dag-draining:
+
+Draining a Dag
+~~~~~~~~~~~~~~
+
+.. versionadded:: 3.4
+
+Pausing is abrupt: queued and downstream tasks of the runs in flight are stranded until the Dag is
+unpaused. To stop starting new runs while letting the current ones complete, for example before an upgrade
+or a maintenance window, set the Dag's scheduling state to ``draining``. A Dag has one of three scheduling
+states: ``active``, ``draining`` or ``paused``. ``draining`` is transient and always converges to ``paused``.
+
+While a Dag is draining:
+
+* the Scheduler creates no new runs for it: neither scheduled runs, nor runs triggered by asset events
+  (the asset events are still recorded);
+* the task instances of its queued and running runs continue to be scheduled and executed, unlike for a paused Dag;
+* explicit triggers (manual, operator-triggered, asset materialization and backfill) remain available, as
+  they are for a paused Dag, and extend the drain until the resulting work finishes.
+
+When the Dag has no queued or running run left, and no backfill that has been accepted but not yet initialized,
+the Scheduler pauses the Dag and records a ``drain_completed`` entry in the audit log.
+
+Use the ``scheduling_state`` field of ``PATCH /dags/{dag_id}`` (or ``PATCH /dags`` for several Dags) to start
+a drain. Clients that only know ``is_paused`` keep working: ``is_paused=true`` pauses the Dag and
+``is_paused=false`` activates it, and both cancel a drain in progress. ``airflow dags pause`` and
+``airflow dags unpause`` do the same.
+
 Dags can be deactivated (do not confuse it with ``Active`` tag in the UI) by removing them from the
 ``DAGS_FOLDER``. When scheduler parses the ``DAGS_FOLDER`` and misses the Dag that it had seen
 before and stored in the database it will set is as deactivated. The metadata and history of the
@@ -848,6 +876,8 @@ we can also provide and override these configuration from Dag argument:
 
 - ``max_consecutive_failed_dag_runs``: Overrides :ref:`config:core__max_consecutive_failed_dag_runs_per_dag`.
 
+Auto-pausing also applies to a draining Dag: it is paused and no longer draining.
+
 Deadline Alerts
 ---------------
 
diff --git a/airflow-core/docs/faq.rst b/airflow-core/docs/faq.rst
index a694f83e53..734ce0548b 100644
--- a/airflow-core/docs/faq.rst
+++ b/airflow-core/docs/faq.rst
@@ -119,6 +119,14 @@ How do I trigger tasks based on another task's failure?
 
 You can achieve this with :ref:`concepts:trigger-rules`.
 
+How do I stop new Dag runs while letting the current ones finish?
+-----------------------------------------------------------------
+
+Set the Dag's scheduling state to ``draining`` with the ``scheduling_state`` field of ``PATCH /dags/{dag_id}``
+instead of pausing it or setting its schedule to ``None``. The Scheduler creates no new runs for a draining Dag,
+keeps scheduling the tasks of its queued and running runs, and pauses the Dag by itself once they have finished.
+See :ref:`concepts:dag-draining`.
+
 .. _faq:how-to-control-dag-file-parsing-timeout:
 
 How to control Dag file parsing timeout for different Dag files?
diff --git a/airflow-core/docs/migrations-ref.rst b/airflow-core/docs/migrations-ref.rst
index e8bef749a6..b14df621f8 100644
--- a/airflow-core/docs/migrations-ref.rst
+++ b/airflow-core/docs/migrations-ref.rst
@@ -39,7 +39,10 @@ Here's the list of all the Database Migrations that are executed via when you ru
 +-------------------------+------------------+-------------------+--------------------------------------------------------------+
 | Revision ID             | Revises ID       | Airflow Version   | Description                                                  |
 +=========================+==================+===================+==============================================================+
-| ``f8c2a1d94e03`` (head) | ``8d3f1a6b2c47`` | ``3.4.0``         | Add team_name and bundle_names scope columns to job table.   |
+| ``a3d9c5e1b7f2`` (head) | ``f8c2a1d94e03`` | ``3.4.0``         | Add is_draining to dag and an index on backfill (dag_id,     |
+|                         |                  |                   | completed_at).                                               |
++-------------------------+------------------+-------------------+--------------------------------------------------------------+
+| ``f8c2a1d94e03``        | ``8d3f1a6b2c47`` | ``3.4.0``         | Add team_name and bundle_names scope columns to job table.   |
 +-------------------------+------------------+-------------------+--------------------------------------------------------------+
 | ``8d3f1a6b2c47``        | ``c7f0a5d2e9b4`` | ``3.4.0``         | Add team_name to log.                                        |
 +-------------------------+------------------+-------------------+--------------------------------------------------------------+
diff --git a/airflow-core/newsfragments/22006.feature.rst b/airflow-core/newsfragments/22006.feature.rst
new file mode 100644
index 0000000000..e0bc94ba97
--- /dev/null
+++ b/airflow-core/newsfragments/22006.feature.rst
@@ -0,0 +1 @@
+Add a "draining" scheduling state for Dags that stops new scheduled runs while the runs already in flight finish, after which the Dag is paused automatically. Set it with ``scheduling_state`` on ``PATCH /dags``; ``is_paused`` keeps working as before.
diff --git a/airflow-core/src/airflow/api_fastapi/common/parameters.py b/airflow-core/src/airflow/api_fastapi/common/parameters.py
index a7a870df99..4bb6532466 100644
--- a/airflow-core/src/airflow/api_fastapi/common/parameters.py
+++ b/airflow-core/src/airflow/api_fastapi/common/parameters.py
@@ -73,7 +73,7 @@ from airflow.models.variable import Variable
 from airflow.models.xcom import XComModel
 from airflow.typing_compat import Self
 from airflow.utils.sqlalchemy import JsonContains, apply_regex_query_timeout
-from airflow.utils.state import DagRunState, TaskInstanceState
+from airflow.utils.state import DagRunState, DagSchedulingState, TaskInstanceState
 from airflow.utils.types import DagRunType
 
 if TYPE_CHECKING:
@@ -1525,6 +1525,31 @@ class _AnyDagRunStateFilter(BaseParam[DagRunState | None]):
         return cls().set_value(dag_run_state)
 
 
+class _SchedulingStateFilter(BaseParam[DagSchedulingState | None]):
+    """Filter Dags by their scheduling state (active, draining or paused)."""
+
+    def to_orm(self, select: Select) -> Select:
+        if self.value is None and self.skip_none:
+            return select
+        return select.where(
+            DagModel.is_paused == (self.value == DagSchedulingState.PAUSED),
+            DagModel.is_draining == (self.value == DagSchedulingState.DRAINING),
+        )
+
+    @classmethod
+    def depends(
+        cls,
+        scheduling_state: DagSchedulingState | None = Query(
+            None,
+            description="Filter Dags by scheduling state: active, draining or paused.",
+        ),
+    ) -> _SchedulingStateFilter:
+        return cls().set_value(scheduling_state)
+
+
+QuerySchedulingStateFilter = Annotated[_SchedulingStateFilter, Depends(_SchedulingStateFilter.depends)]
+
+
 # DagRun
 QueryLastDagRunStateFilter = Annotated[
     FilterParam[DagRunState | None],
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py b/airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py
index e67777d6ff..f7c38ae859 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py
+++ b/airflow-core/src/airflow/api_fastapi/core_api/datamodels/dags.py
@@ -31,15 +31,17 @@ from pydantic import (
     computed_field,
     field_serializer,
     field_validator,
+    model_validator,
 )
 
 from airflow._shared.module_loading import qualname
-from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel, make_partial_model
+from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
 from airflow.api_fastapi.core_api.datamodels.common import MaybeAssetExpression
 from airflow.api_fastapi.core_api.datamodels.dag_tags import DagTagResponse
 from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
 from airflow.configuration import conf
 from airflow.models.dag_version import DagVersion
+from airflow.utils.state import DagSchedulingState
 from airflow.utils.types import DagRunType
 
 if TYPE_CHECKING:
@@ -84,6 +86,7 @@ class DAGResponse(BaseModel):
     dag_id: str
     dag_display_name: str
     is_paused: bool
+    scheduling_state: DagSchedulingState = DagSchedulingState.ACTIVE
     is_stale: bool
     last_parsed_time: datetime | None
     last_parse_duration: float | None
@@ -160,12 +163,22 @@ class DAGResponse(BaseModel):
 
 
 class DAGPatchBody(StrictBaseModel):
-    """Dag Serializer for updatable bodies."""
-
-    is_paused: bool
-
-
-DAGPatchBodyPartial = make_partial_model(DAGPatchBody)
+    """Dag Serializer for updatable bodies; exactly one of the two fields must be set."""
+
+    is_paused: bool | None = None
+    scheduling_state: DagSchedulingState | None = None
+
+    @model_validator(mode="after")
+    def validate_exactly_one_field(self) -> DAGPatchBody:
+        if (self.is_paused is None) == (self.scheduling_state is None):
+            raise ValueError("Exactly one of `is_paused` or `scheduling_state` must be provided")
+        return self
+
+    def get_target_scheduling_state(self) -> DagSchedulingState:
+        """Return the scheduling state requested by whichever field the client sent."""
+        if self.scheduling_state is not None:
+            return self.scheduling_state
+        return DagSchedulingState.PAUSED if self.is_paused else DagSchedulingState.ACTIVE
 
 
 class DAGCollectionResponse(BaseModel):
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py b/airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py
index 6612906a6e..edfc820f0c 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py
+++ b/airflow-core/src/airflow/api_fastapi/core_api/datamodels/ui/dags.py
@@ -32,6 +32,7 @@ class DAGWithLatestDagRunsResponse(DAGResponse):
     latest_dag_runs: list[DAGRunLightResponse]
     pending_actions: list[HITLDetail]
     is_favorite: bool
+    has_unfinished_runs: bool = False
     team_name: str | None = None
 
 
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml b/airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml
index a51f805f09..680428838e 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml
+++ b/airflow-core/src/airflow/api_fastapi/core_api/openapi/_private_ui.yaml
@@ -641,6 +641,16 @@ paths:
           - type: boolean
           - type: 'null'
           title: Paused
+      - name: scheduling_state
+        in: query
+        required: false
+        schema:
+          anyOf:
+          - $ref: '#/components/schemas/DagSchedulingState'
+          - type: 'null'
+          description: 'Filter Dags by scheduling state: active, draining or paused.'
+          title: Scheduling State
+        description: 'Filter Dags by scheduling state: active, draining or paused.'
       - name: has_import_errors
         in: query
         required: false
@@ -3090,6 +3100,9 @@ components:
         is_paused:
           type: boolean
           title: Is Paused
+        scheduling_state:
+          $ref: '#/components/schemas/DagSchedulingState'
+          default: active
         is_stale:
           type: boolean
           title: Is Stale
@@ -3232,6 +3245,10 @@ components:
         is_favorite:
           type: boolean
           title: Is Favorite
+        has_unfinished_runs:
+          type: boolean
+          title: Has Unfinished Runs
+          default: false
         team_name:
           anyOf:
           - type: string
@@ -3365,6 +3382,19 @@ components:
       - updated_at
       title: DagScheduleAssetReference
       description: Dag schedule reference serializer for assets.
+    DagSchedulingState:
+      type: string
+      enum:
+      - active
+      - draining
+      - paused
+      title: DagSchedulingState
+      description: 'Scheduling state of a Dag.
+
+
+        ``DRAINING`` is transient: the scheduler stops creating new runs and moves the Dag
+
+        to ``PAUSED`` once its in-flight work has finished.'
     DagTagResponse:
       properties:
         name:
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml b/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml
index bea12275f6..4135cdbb60 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml
+++ b/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml
@@ -12958,6 +12958,9 @@ components:
         is_paused:
           type: boolean
           title: Is Paused
+        scheduling_state:
+          $ref: '#/components/schemas/DagSchedulingState'
+          default: active
         is_stale:
           type: boolean
           title: Is Stale
@@ -13253,14 +13256,18 @@ components:
     DAGPatchBody:
       properties:
         is_paused:
-          type: boolean
+          anyOf:
+          - type: boolean
+          - type: 'null'
           title: Is Paused
+        scheduling_state:
+          anyOf:
+          - $ref: '#/components/schemas/DagSchedulingState'
+          - type: 'null'
       additionalProperties: false
       type: object
-      required:
-      - is_paused
       title: DAGPatchBody
-      description: Dag Serializer for updatable bodies.
+      description: Dag Serializer for updatable bodies; exactly one of the two fields must be set.
     DAGResponse:
       properties:
         dag_id:
@@ -13272,6 +13279,9 @@ components:
         is_paused:
           type: boolean
           title: Is Paused
+        scheduling_state:
+          $ref: '#/components/schemas/DagSchedulingState'
+          default: active
         is_stale:
           type: boolean
           title: Is Stale
@@ -14141,6 +14151,19 @@ components:
       - updated_at
       title: DagScheduleAssetReference
       description: Dag schedule reference serializer for assets.
+    DagSchedulingState:
+      type: string
+      enum:
+      - active
+      - draining
+      - paused
+      title: DagSchedulingState
+      description: 'Scheduling state of a Dag.
+
+
+        ``DRAINING`` is transient: the scheduler stops creating new runs and moves the Dag
+
+        to ``PAUSED`` once its in-flight work has finished.'
     DagStatsCollectionResponse:
       properties:
         dags:
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py b/airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py
index 2140707ab2..874fed4b1a 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py
+++ b/airflow-core/src/airflow/api_fastapi/core_api/routes/public/dags.py
@@ -20,8 +20,6 @@ from __future__ import annotations
 from typing import Annotated
 
 from fastapi import Depends, HTTPException, Query, Response, status
-from fastapi.exceptions import RequestValidationError
-from pydantic import ValidationError
 from sqlalchemy import delete, func, insert, select, update
 
 from airflow.api.common import delete_dag as delete_dag_module
@@ -62,7 +60,6 @@ from airflow.api_fastapi.core_api.datamodels.dags import (
     DAGCollectionResponse,
     DAGDetailsResponse,
     DAGPatchBody,
-    DAGPatchBodyPartial,
     DAGResponse,
 )
 from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
@@ -77,10 +74,27 @@ from airflow.exceptions import AirflowException, DagNotFound
 from airflow.models import DagModel
 from airflow.models.dag_favorite import DagFavorite
 from airflow.models.dagrun import DagRun
-from airflow.utils.state import DagRunState
+from airflow.utils.state import DagRunState, DagSchedulingState
 
 dags_router = AirflowRouter(tags=["DAG"], prefix="/dags")
 
+PATCHABLE_FIELDS = ("is_paused", "scheduling_state")
+
+
+def _validate_update_mask(patch_body: DAGPatchBody, update_mask: list[str] | None) -> None:
+    if not update_mask:
+        return
+    if len(update_mask) != 1 or update_mask[0] not in PATCHABLE_FIELDS:
+        raise HTTPException(
+            status.HTTP_400_BAD_REQUEST,
+            "Only `is_paused` or `scheduling_state` can be updated through the REST API",
+        )
+    if getattr(patch_body, update_mask[0]) is None:
+        raise HTTPException(
+            HTTP_422_UNPROCESSABLE_CONTENT,
+            f"`update_mask` names `{update_mask[0]}` but the request body does not set it",
+        )
+
 
 @dags_router.get("", dependencies=[Depends(requires_access_dag(method="GET"))])
 def get_dags(
@@ -294,27 +308,8 @@ def patch_dag(
     if dag is None:
         raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id: {dag_id} was not found")
 
-    fields_to_update = patch_body.model_fields_set
-    if update_mask:
-        if update_mask != ["is_paused"]:
-            raise HTTPException(
-                status.HTTP_400_BAD_REQUEST, "Only `is_paused` field can be updated through the REST API"
-            )
-        fields_to_update = fields_to_update.intersection(update_mask)
-        try:
-            DAGPatchBodyPartial(**patch_body.model_dump(include=fields_to_update))
-        except ValidationError as e:
-            raise RequestValidationError(errors=e.errors())
-    else:
-        try:
-            DAGPatchBody(**patch_body.model_dump())
-        except ValidationError as e:
-            raise RequestValidationError(errors=e.errors())
-
-    data = patch_body.model_dump(include=fields_to_update, by_alias=True)
-
-    for key, val in data.items():
-        setattr(dag, key, val)
+    _validate_update_mask(patch_body, update_mask)
+    dag.set_scheduling_state(patch_body.get_target_scheduling_state())
 
     return dag
 
@@ -350,16 +345,7 @@ def patch_dags(
     matched regardless of other filters. To match all Dags, pass a wildcard value such as
     `~` or `%` for `dag_id_pattern`.
     """
-    if update_mask:
-        if update_mask != ["is_paused"]:
-            raise HTTPException(
-                status.HTTP_400_BAD_REQUEST, "Only `is_paused` field can be updated through the REST API"
-            )
-    else:
-        try:
-            DAGPatchBody.model_validate(patch_body)
-        except ValidationError as e:
-            raise RequestValidationError(errors=e.errors())
+    _validate_update_mask(patch_body, update_mask)
 
     dags_select, total_entries = paginated_select(
         statement=select(DagModel),
@@ -392,10 +378,14 @@ def patch_dags(
         ],
     ).subquery()
 
+    target_state = patch_body.get_target_scheduling_state()
     session.execute(
         update(DagModel)
         .where(DagModel.dag_id.in_(select(filtered_dag_ids.c.dag_id)))
-        .values(is_paused=patch_body.is_paused)
+        .values(
+            is_paused=target_state == DagSchedulingState.PAUSED,
+            is_draining=target_state == DagSchedulingState.DRAINING,
+        )
         .execution_options(synchronize_session="fetch")
     )
 
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py b/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py
index 78ed45d973..1868148e85 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py
+++ b/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dags.py
@@ -51,6 +51,7 @@ from airflow.api_fastapi.common.parameters import (
     QueryOwnersFilter,
     QueryPausedFilter,
     QueryPendingActionsFilter,
+    QuerySchedulingStateFilter,
     QueryTagsFilter,
     QueryTeamsFilter,
     QueryTimetableTypePrefixPatternSearch,
@@ -113,6 +114,7 @@ def get_dags(
     dag_display_name_prefix_pattern: QueryDagDisplayNamePrefixPatternSearch,
     exclude_stale: QueryExcludeStaleFilter,
     paused: QueryPausedFilter,
+    scheduling_state: QuerySchedulingStateFilter,
     has_import_errors: QueryHasImportErrorsFilter,
     last_dag_run_state: QueryLastDagRunStateFilter,
     dag_run_state: QueryAnyDagRunStateFilter,
@@ -160,6 +162,7 @@ def get_dags(
         filters=[
             exclude_stale,
             paused,
+            scheduling_state,
             has_import_errors,
             dag_id_pattern,
             dag_id_prefix_pattern,
@@ -195,6 +198,19 @@ def get_dags(
     )
     favorite_dag_ids = set(session.scalars(favorites_select))
 
+    dag_ids_with_unfinished_runs: set[str] = set()
+    if dags:
+        dag_ids_with_unfinished_runs = set(
+            session.scalars(
+                select(DagRun.dag_id)
+                .where(
+                    DagRun.dag_id.in_([dag.dag_id for dag in dags]),
+                    DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)),
+                )
+                .distinct()
+            )
+        )
+
     recent_dag_runs: list = []
     if dags:
         recent_runs_branches = [
@@ -269,6 +285,7 @@ def get_dags(
                 "latest_dag_runs": [],
                 "pending_actions": pending_actions_by_dag_id[dag.dag_id],
                 "is_favorite": dag.dag_id in favorite_dag_ids,
+                "has_unfinished_runs": dag.dag_id in dag_ids_with_unfinished_runs,
                 "team_name": team_names_by_dag_id.get(dag.dag_id),
             }
         )
diff --git a/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py b/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py
index 483cbc009d..94b177f632 100644
--- a/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py
+++ b/airflow-core/src/airflow/api_fastapi/core_api/routes/ui/dashboard.py
@@ -20,7 +20,7 @@ from decimal import ROUND_FLOOR, Context
 from typing import TYPE_CHECKING, cast
 
 from fastapi import Depends, status
-from sqlalchemy import func, select
+from sqlalchemy import and_, func, select
 from sqlalchemy.sql.expression import case, false
 
 from airflow._shared.timezones import timezone
@@ -152,7 +152,7 @@ def dag_stats(
     )
     dag_counts_query = (
         select(
-            func.coalesce(func.sum(case((DagModel.is_paused == false(), 1))), 0).label("active"),
+            func.coalesce(func.sum(case((and_(DagModel.is_paused == false(), DagModel.is_draining == false()), 1))), 0).label("active"),
             func.coalesce(func.sum(case((latest_state == DagRunState.FAILED, 1))), 0).label("failed"),
         )
         .select_from(DagModel)
diff --git a/airflow-core/src/airflow/assets/manager.py b/airflow-core/src/airflow/assets/manager.py
index a417ad96ea..1aec22d30f 100644
--- a/airflow-core/src/airflow/assets/manager.py
+++ b/airflow-core/src/airflow/assets/manager.py
@@ -351,7 +351,9 @@ class AssetManager(LoggingMixin):
         session.add(asset_event)
         session.flush()
 
-        dags_to_queue_from_asset = {ref.dag for ref in asset_model.scheduled_dags if not ref.dag.is_paused}
+        dags_to_queue_from_asset = {
+            ref.dag for ref in asset_model.scheduled_dags if not (ref.dag.is_paused or ref.dag.is_draining)
+        }
 
         dags_to_queue_from_asset_alias = set()
         if source_alias_names:
@@ -379,7 +381,7 @@ class AssetManager(LoggingMixin):
                 dags_to_queue_from_asset_alias |= {
                     alias_ref.dag
                     for alias_ref in asset_alias_model.scheduled_dags
-                    if not alias_ref.dag.is_paused
+                    if not (alias_ref.dag.is_paused or alias_ref.dag.is_draining)
                 }
         else:
             asset_alias_models = []
@@ -395,6 +397,7 @@ class AssetManager(LoggingMixin):
                         DagScheduleAssetUriReference.uri == asset.uri,
                     ),
                     DagModel.is_paused.is_(False),
+                    DagModel.is_draining.is_(False),
                 )
             )
         )
diff --git a/airflow-core/src/airflow/cli/commands/dag_command.py b/airflow-core/src/airflow/cli/commands/dag_command.py
index a8ea2ccdd6..949e2b9433 100644
--- a/airflow-core/src/airflow/cli/commands/dag_command.py
+++ b/airflow-core/src/airflow/cli/commands/dag_command.py
@@ -30,7 +30,7 @@ import subprocess
 import sys
 from typing import TYPE_CHECKING, cast
 
-from sqlalchemy import func, select
+from sqlalchemy import func, or_, select
 
 from airflow._shared.timezones import timezone
 from airflow.api.client import get_current_api_client
@@ -59,7 +59,7 @@ from airflow.utils.helpers import ask_yesno, chunks
 from airflow.utils.platform import getuser
 from airflow.utils.providers_configuration_loader import providers_configuration_loaded
 from airflow.utils.session import NEW_SESSION, create_session, provide_session
-from airflow.utils.state import DagRunState, TaskInstanceState
+from airflow.utils.state import DagRunState, DagSchedulingState, TaskInstanceState
 from airflow.utils.types import DagRunType
 
 if TYPE_CHECKING:
@@ -270,7 +270,7 @@ def set_is_paused(is_paused: bool, args, dag: DAG | None = None, *, session: Ses
     else:
         query = query.where(DagModel.dag_id == args.dag_id)
 
-    query = query.where(DagModel.is_paused != is_paused)
+    query = query.where(or_(DagModel.is_paused != is_paused, DagModel.is_draining.is_(True)))
 
     matched_dags = list(session.scalars(query).all())
     if not matched_dags:
@@ -288,9 +288,11 @@ def set_is_paused(is_paused: bool, args, dag: DAG | None = None, *, session: Ses
             print("Operation cancelled by user")
             return
 
+    target_state = DagSchedulingState.PAUSED if is_paused else DagSchedulingState.ACTIVE
+
     def _update_is_paused(dag_model: DagModel) -> bool:
         old_is_paused = dag_model.is_paused
-        dag_model.is_paused = is_paused
+        dag_model.set_scheduling_state(target_state)
         return old_is_paused
 
     old_values = [
@@ -381,6 +383,7 @@ def _get_dagbag_dag_details(dag: DAG) -> dict:
         "bundle_name": None,
         "bundle_version": None,
         "is_paused": None,
+        "scheduling_state": None,
         "is_stale": None,
         "last_parsed_time": None,
         "last_parse_duration": None,
diff --git a/airflow-core/src/airflow/jobs/scheduler_job_runner.py b/airflow-core/src/airflow/jobs/scheduler_job_runner.py
index a082334f2a..db58200197 100644
--- a/airflow-core/src/airflow/jobs/scheduler_job_runner.py
+++ b/airflow-core/src/airflow/jobs/scheduler_job_runner.py
@@ -131,7 +131,7 @@ from airflow.utils.sqlalchemy import (
     random_db_uuid,
     with_row_locks,
 )
-from airflow.utils.state import CallbackState, DagRunState, State, TaskInstanceState
+from airflow.utils.state import CallbackState, DagRunState, DagSchedulingState, State, TaskInstanceState
 from airflow.utils.types import DagRunTriggeredByType, DagRunType
 
 if TYPE_CHECKING:
@@ -1831,6 +1831,8 @@ class SchedulerJobRunner(BaseJobRunner, LoggingMixin):
             self._mark_backfills_complete,
         )
 
+        timers.call_regular_interval(5.0, self._finalize_draining_dags)
+
         if self._is_metrics_enabled() or self._is_tracing_enabled():
             timers.call_regular_interval(
                 conf.getfloat("scheduler", "pool_metrics_interval", fallback=5.0),
@@ -2282,6 +2284,7 @@ class SchedulerJobRunner(BaseJobRunner, LoggingMixin):
                 .where(
                     AssetPartitionDagRun.created_dag_run_id.is_(None),
                     DagModel.is_stale.is_(False),
+                    DagModel.is_draining.is_(False),
                 )
                 .order_by(AssetPartitionDagRun.created_at, AssetPartitionDagRun.id)
                 .limit(self._max_partition_dag_runs_per_loop),
@@ -2520,6 +2523,45 @@ class SchedulerJobRunner(BaseJobRunner, LoggingMixin):
         for b in backfills:
             b.completed_at = now
 
+    @provide_session
+    def _finalize_draining_dags(self, session: Session = NEW_SESSION) -> None:
+        """Pause draining Dags whose in-flight work has finished."""
+        unfinished_states = (DagRunState.RUNNING, DagRunState.QUEUED)
+        query = (
+            select(DagModel)
+            .where(
+                DagModel.is_draining.is_(True),
+                ~exists(
+                    select(DagRun.id).where(
+                        DagRun.dag_id == DagModel.dag_id, DagRun.state.in_(unfinished_states)
+                    )
+                ),
+                # A backfill is committed before its Dag runs exist, so it must hold the drain open.
+                ~exists(
+                    select(Backfill.id).where(
+                        Backfill.dag_id == DagModel.dag_id,
+                        Backfill.completed_at.is_(None),
+                        ~exists(select(BackfillDagRun.id).where(BackfillDagRun.backfill_id == Backfill.id)),
+                    )
+                ),
+            )
+            .order_by(DagModel.dag_id)
+            .limit(DagModel.NUM_DAGS_PER_DAGRUN_QUERY)
+        )
+        draining_dags = session.scalars(with_row_locks(query, of=DagModel, session=session, skip_locked=True))
+        for dag_model in draining_dags:
+            dag_model.set_scheduling_state(DagSchedulingState.PAUSED)
+            session.add(
+                Log(
+                    event="drain_completed",
+                    dag_id=dag_model.dag_id,
+                    owner="scheduler",
+                    owner_display_name="Scheduler",
+                    extra=f"[('dag_id', '{dag_model.dag_id}'), ('is_paused', True)]",
+                )
+            )
+            self.log.info("Dag %s finished draining and is now paused", dag_model.dag_id)
+
     def _create_dag_runs(self, dag_models: Collection[DagModel], session: Session) -> None:
         """Create a DAG run and update the dag_model to control if/when the next DAGRun should be created."""
         # Bulk Fetch DagRuns with dag_id and logical_date same
diff --git a/airflow-core/src/airflow/migrations/versions/0134_3_4_0_add_dag_is_draining.py b/airflow-core/src/airflow/migrations/versions/0134_3_4_0_add_dag_is_draining.py
new file mode 100644
index 0000000000..9f172f4a7e
--- /dev/null
+++ b/airflow-core/src/airflow/migrations/versions/0134_3_4_0_add_dag_is_draining.py
@@ -0,0 +1,62 @@
+#
+# Licensed to the Apache Software Foundation (ASF) under one
+# or more contributor license agreements.  See the NOTICE file
+# distributed with this work for additional information
+# regarding copyright ownership.  The ASF licenses this file
+# to you under the Apache License, Version 2.0 (the
+# "License"); you may not use this file except in compliance
+# with the License.  You may obtain a copy of the License at
+#
+#   http://www.apache.org/licenses/LICENSE-2.0
+#
+# Unless required by applicable law or agreed to in writing,
+# software distributed under the License is distributed on an
+# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
+# KIND, either express or implied.  See the License for the
+# specific language governing permissions and limitations
+# under the License.
+
+"""
+Add is_draining to dag and an index on backfill (dag_id, completed_at).
+
+Revision ID: a3d9c5e1b7f2
+Revises: f8c2a1d94e03
+Create Date: 2026-07-15 02:00:00.000000
+
+"""
+
+from __future__ import annotations
+
+import sqlalchemy as sa
+from alembic import op
+
+# revision identifiers, used by Alembic.
+revision = "a3d9c5e1b7f2"
+down_revision = "f8c2a1d94e03"
+branch_labels = None
+depends_on = None
+airflow_version = "3.4.0"
+
+
+def upgrade():
+    """Add dag.is_draining with index and check constraint, and the backfill index."""
+    with op.batch_alter_table("dag", schema=None) as batch_op:
+        batch_op.add_column(sa.Column("is_draining", sa.Boolean(), nullable=False, server_default="0"))
+        batch_op.create_index("idx_dag_is_draining", ["is_draining"], unique=False)
+        batch_op.create_check_constraint(
+            batch_op.f("ck_dag_dag_not_paused_and_draining"), sa.text("NOT (is_paused AND is_draining)")
+        )
+
+    with op.batch_alter_table("backfill", schema=None) as batch_op:
+        batch_op.create_index("idx_backfill_dag_id_completed_at", ["dag_id", "completed_at"], unique=False)
+
+
+def downgrade():
+    """Remove dag.is_draining and the backfill index."""
+    with op.batch_alter_table("backfill", schema=None) as batch_op:
+        batch_op.drop_index("idx_backfill_dag_id_completed_at")
+
+    with op.batch_alter_table("dag", schema=None) as batch_op:
+        batch_op.drop_constraint(batch_op.f("ck_dag_dag_not_paused_and_draining"), type_="check")
+        batch_op.drop_index("idx_dag_is_draining")
+        batch_op.drop_column("is_draining")
diff --git a/airflow-core/src/airflow/models/backfill.py b/airflow-core/src/airflow/models/backfill.py
index 203be25188..28e173fc0c 100644
--- a/airflow-core/src/airflow/models/backfill.py
+++ b/airflow-core/src/airflow/models/backfill.py
@@ -33,6 +33,7 @@ import structlog
 from sqlalchemy import (
     Boolean,
     ForeignKeyConstraint,
+    Index,
     Integer,
     String,
     UniqueConstraint,
@@ -188,6 +189,8 @@ class Backfill(Base):
         nullable=True,
     )  # The user that triggered the Backfill, if applicable
 
+    __table_args__ = (Index("idx_backfill_dag_id_completed_at", dag_id, completed_at),)
+
     backfill_dag_run_associations = relationship("BackfillDagRun", back_populates="backfill")
 
     dag_model = relationship(
diff --git a/airflow-core/src/airflow/models/dag.py b/airflow-core/src/airflow/models/dag.py
index 4c8d4d3e68..6210892851 100644
--- a/airflow-core/src/airflow/models/dag.py
+++ b/airflow-core/src/airflow/models/dag.py
@@ -30,6 +30,7 @@ from cachetools import TTLCache, cached
 from dateutil.relativedelta import relativedelta
 from sqlalchemy import (
     Boolean,
+    CheckConstraint,
     Float,
     ForeignKey,
     Index,
@@ -73,7 +74,7 @@ from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataInte
 from airflow.timetables.simple import AssetTriggeredTimetable, NullTimetable, OnceTimetable
 from airflow.utils.session import NEW_SESSION, provide_session
 from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
-from airflow.utils.state import DagRunState
+from airflow.utils.state import DagRunState, DagSchedulingState
 from airflow.utils.types import DagRunType
 
 if TYPE_CHECKING:
@@ -321,6 +322,8 @@ class DagModel(Base):
     # Set this default value of is_paused based on a configuration value!
     is_paused_at_creation = airflow_conf.getboolean("core", "dags_are_paused_at_creation")
     is_paused: Mapped[bool] = mapped_column(Boolean, default=is_paused_at_creation)
+    # Transient: no new scheduler-created runs; the scheduler pauses the Dag once in-flight work is done
+    is_draining: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
     # Whether that DAG was seen on the last DagBag load
     is_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
     exceeds_max_non_backfill: Mapped[bool] = mapped_column(
@@ -405,7 +408,27 @@ class DagModel(Base):
     # Earliest time at which this ``next_dagrun`` can be created.
     next_dagrun_create_after: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
 
-    __table_args__ = (Index("idx_next_dagrun_create_after", next_dagrun_create_after, unique=False),)
+    __table_args__ = (
+        Index("idx_next_dagrun_create_after", next_dagrun_create_after, unique=False),
+        Index("idx_dag_is_draining", is_draining),
+        CheckConstraint(
+            sa.text("NOT (is_paused AND is_draining)"),
+            name="dag_not_paused_and_draining",
+        ),
+    )
+
+    @property
+    def scheduling_state(self) -> DagSchedulingState:
+        if self.is_draining:
+            return DagSchedulingState.DRAINING
+        if self.is_paused:
+            return DagSchedulingState.PAUSED
+        return DagSchedulingState.ACTIVE
+
+    def set_scheduling_state(self, state: DagSchedulingState | str) -> None:
+        state = DagSchedulingState(state)
+        self.is_paused = state == DagSchedulingState.PAUSED
+        self.is_draining = state == DagSchedulingState.DRAINING
 
     schedule_asset_references = relationship(
         "DagScheduleAssetReference",
@@ -774,6 +797,7 @@ class DagModel(Base):
             select(cls)
             .where(
                 cls.is_paused == expression.false(),
+                cls.is_draining == expression.false(),
                 cls.is_stale == expression.false(),
                 cls.has_import_errors == expression.false(),
                 cls.exceeds_max_non_backfill == expression.false(),
diff --git a/airflow-core/src/airflow/models/dagrun.py b/airflow-core/src/airflow/models/dagrun.py
index 725bf09c77..28251b6a01 100644
--- a/airflow-core/src/airflow/models/dagrun.py
+++ b/airflow-core/src/airflow/models/dagrun.py
@@ -1026,7 +1026,7 @@ class DagRun(Base, LoggingMixin):
             session.execute(
                 update(DagModel)
                 .where(or_(*filter_query))
-                .values(is_paused=True)
+                .values(is_paused=True, is_draining=False)
                 .execution_options(synchronize_session="fetch")
             )
             session.add(
diff --git a/airflow-core/src/airflow/utils/db.py b/airflow-core/src/airflow/utils/db.py
index f0779c9969..082c23031a 100644
--- a/airflow-core/src/airflow/utils/db.py
+++ b/airflow-core/src/airflow/utils/db.py
@@ -117,7 +117,7 @@ _REVISION_HEADS_MAP: dict[str, str] = {
     "3.1.8": "509b94a1042d",
     "3.2.0": "1d6611b6ab7c",
     "3.3.0": "d2f4e1b3c5a7",
-    "3.4.0": "f8c2a1d94e03",
+    "3.4.0": "a3d9c5e1b7f2",
 }
 
 # Prefix used to identify tables holding data moved during migration.
diff --git a/airflow-core/src/airflow/utils/state.py b/airflow-core/src/airflow/utils/state.py
index 89c8efcaa3..71bd0c4071 100644
--- a/airflow-core/src/airflow/utils/state.py
+++ b/airflow-core/src/airflow/utils/state.py
@@ -112,6 +112,22 @@ class DagRunState(str, Enum):
         return self.value
 
 
+class DagSchedulingState(str, Enum):
+    """
+    Scheduling state of a Dag.
+
+    ``DRAINING`` is transient: the scheduler stops creating new runs and moves the Dag
+    to ``PAUSED`` once its in-flight work has finished.
+    """
+
+    ACTIVE = "active"
+    DRAINING = "draining"
+    PAUSED = "paused"
+
+    def __str__(self) -> str:
+        return self.value
+
+
 class State:
     """Static class with task instance state constants and color methods to avoid hard-coding."""
 
diff --git a/airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py b/airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py
index 9536a1719e..3a81ad8fde 100644
--- a/airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py
+++ b/airflow-core/tests/unit/api_fastapi/core_api/datamodels/test_dags.py
@@ -19,12 +19,15 @@ from __future__ import annotations
 from datetime import datetime, timedelta, timezone
 
 import pytest
+from pydantic import ValidationError
 
 from airflow._shared.module_loading import qualname
 from airflow.api_fastapi.core_api.datamodels.dags import (
     DAGDetailsResponse,
+    DAGPatchBody,
     DAGResponse,
 )
+from airflow.utils.state import DagSchedulingState
 from airflow.utils.types import DagRunType
 
 
@@ -135,6 +138,48 @@ class TestGetDefaultArgsValidator:
         assert result["connection"] is value
 
 
+class TestSchedulingState:
+    def test_response_defaults_to_active(self):
+        assert _make_dag_response().scheduling_state == DagSchedulingState.ACTIVE
+
+    def test_response_carries_scheduling_state(self):
+        dag = _make_dag_response(scheduling_state="draining")
+        assert dag.model_dump()["scheduling_state"] == DagSchedulingState.DRAINING
+
+
+class TestDAGPatchBody:
+    @pytest.mark.parametrize(
+        ("body", "expected_state"),
+        [
+            pytest.param({"is_paused": True}, DagSchedulingState.PAUSED, id="is_paused-true"),
+            pytest.param({"is_paused": False}, DagSchedulingState.ACTIVE, id="is_paused-false"),
+            pytest.param({"scheduling_state": "draining"}, DagSchedulingState.DRAINING, id="draining"),
+            pytest.param(
+                {"is_paused": None, "scheduling_state": "paused"}, DagSchedulingState.PAUSED, id="null-is_paused"
+            ),
+            pytest.param(
+                {"is_paused": True, "scheduling_state": None}, DagSchedulingState.PAUSED, id="null-state"
+            ),
+        ],
+    )
+    def test_exactly_one_field_is_accepted(self, body, expected_state):
+        assert DAGPatchBody.model_validate(body).get_target_scheduling_state() == expected_state
+
+    @pytest.mark.parametrize(
+        "body",
+        [
+            pytest.param({"is_paused": True, "scheduling_state": "paused"}, id="both"),
+            pytest.param({}, id="neither"),
+            pytest.param({"is_paused": None}, id="only-null-is_paused"),
+            pytest.param({"scheduling_state": None}, id="only-null-state"),
+            pytest.param({"is_paused": None, "scheduling_state": None}, id="both-null"),
+        ],
+    )
+    def test_other_combinations_are_rejected(self, body):
+        with pytest.raises(ValidationError):
+            DAGPatchBody.model_validate(body)
+
+
 class TestIsBackfillable:
     @pytest.mark.parametrize(
         "timetable_periodic",
diff --git a/airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py b/airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py
index b37ccf1a13..601430bd65 100644
--- a/airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py
+++ b/airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py
@@ -31,7 +31,7 @@ from airflow.models.dagbundle import DagBundleModel
 from airflow.models.dagrun import DagRun
 from airflow.models.team import Team
 from airflow.providers.standard.operators.empty import EmptyOperator
-from airflow.utils.state import DagRunState, TaskInstanceState
+from airflow.utils.state import DagRunState, DagSchedulingState, TaskInstanceState
 from airflow.utils.types import DagRunTriggeredByType, DagRunType
 
 from tests_common.test_utils.asserts import assert_queries_count, capture_orm_selects, count_queries
@@ -963,9 +963,130 @@ class TestPatchDag(TestDagEndpoint):
         )
 
 
+    @pytest.mark.parametrize(
+        ("body", "query_params", "expected_state", "expected_is_paused"),
+        [
+            pytest.param({"scheduling_state": "draining"}, {}, "draining", False, id="draining"),
+            pytest.param({"scheduling_state": "paused"}, {}, "paused", True, id="paused"),
+            pytest.param({"scheduling_state": "active"}, {}, "active", False, id="active"),
+            pytest.param(
+                {"scheduling_state": "draining", "is_paused": None}, {}, "draining", False, id="null-is_paused"
+            ),
+            pytest.param(
+                {"scheduling_state": "draining"},
+                {"update_mask": ["scheduling_state"]},
+                "draining",
+                False,
+                id="mask-scheduling_state",
+            ),
+            pytest.param(
+                {"is_paused": True, "scheduling_state": None}, {}, "paused", True, id="null-scheduling_state"
+            ),
+        ],
+    )
+    def test_patch_dag_scheduling_state(
+        self, test_client, session, body, query_params, expected_state, expected_is_paused
+    ):
+        response = test_client.patch(f"/dags/{DAG2_ID}", json=body, params=query_params)
+
+        assert response.status_code == 200
+        assert response.json()["scheduling_state"] == expected_state
+        assert response.json()["is_paused"] is expected_is_paused
+        session.expire_all()
+        assert session.get(DagModel, DAG2_ID).scheduling_state == expected_state
+
+    @pytest.mark.parametrize(
+        ("body", "expected_state"),
+        [({"is_paused": True}, "paused"), ({"is_paused": False}, "active")],
+    )
+    def test_patch_dag_is_paused_clears_draining(self, test_client, session, body, expected_state):
+        session.get(DagModel, DAG2_ID).set_scheduling_state(DagSchedulingState.DRAINING)
+        session.commit()
+
+        response = test_client.patch(f"/dags/{DAG2_ID}", json=body)
+
+        assert response.status_code == 200
+        assert response.json()["scheduling_state"] == expected_state
+        session.expire_all()
+        assert session.get(DagModel, DAG2_ID).is_draining is False
+
+    @pytest.mark.parametrize(
+        "body",
+        [
+            pytest.param({"is_paused": True, "scheduling_state": "paused"}, id="both"),
+            pytest.param({}, id="neither"),
+            pytest.param({"is_paused": None}, id="only-null-is_paused"),
+            pytest.param({"scheduling_state": None}, id="only-null-scheduling_state"),
+            pytest.param({"is_paused": None, "scheduling_state": None}, id="both-null"),
+        ],
+    )
+    def test_patch_dag_requires_exactly_one_field(self, test_client, body):
+        response = test_client.patch(f"/dags/{DAG2_ID}", json=body)
+        assert response.status_code == 422
+
+    @pytest.mark.parametrize(
+        ("body", "update_mask", "expected_status_code"),
+        [
+            pytest.param({"is_paused": True}, ["scheduling_state"], 422, id="state-mask-is_paused-body"),
+            pytest.param({"scheduling_state": "paused"}, ["is_paused"], 422, id="is_paused-mask-state-body"),
+            pytest.param({"is_paused": True}, ["is_paused", "scheduling_state"], 400, id="both-in-mask"),
+            pytest.param({"is_paused": True}, ["description"], 400, id="unsupported-field"),
+        ],
+    )
+    def test_patch_dag_update_mask_must_match_body(
+        self, test_client, body, update_mask, expected_status_code
+    ):
+        response = test_client.patch(f"/dags/{DAG2_ID}", json=body, params={"update_mask": update_mask})
+        assert response.status_code == expected_status_code
+
+
 class TestPatchDags(TestDagEndpoint):
     """Unit tests for Patch DAGs."""
 
+    @pytest.mark.parametrize(
+        ("body", "update_mask", "expected_status_code"),
+        [
+            pytest.param({"scheduling_state": "draining"}, ["scheduling_state"], 200, id="state-mask-state"),
+            pytest.param({"is_paused": True}, ["scheduling_state"], 422, id="state-mask-is_paused-body"),
+            pytest.param({"scheduling_state": "paused"}, ["is_paused"], 422, id="is_paused-mask-state-body"),
+            pytest.param({"scheduling_state": "paused", "is_paused": True}, None, 422, id="both"),
+            pytest.param({}, None, 422, id="neither"),
+        ],
+    )
+    def test_patch_dags_update_mask_and_body_validation(
+        self, test_client, body, update_mask, expected_status_code
+    ):
+        params = {"dag_id_pattern": "~"}
+        if update_mask:
+            params["update_mask"] = update_mask
+        response = test_client.patch("/dags", json=body, params=params)
+        assert response.status_code == expected_status_code
+
+    @pytest.mark.parametrize(
+        ("body", "initial_state", "expected_state"),
+        [
+            pytest.param({"scheduling_state": "draining"}, DagSchedulingState.ACTIVE, "draining", id="drain"),
+            pytest.param(
+                {"scheduling_state": "draining"}, DagSchedulingState.PAUSED, "draining", id="drain-paused"
+            ),
+            pytest.param({"scheduling_state": "active"}, DagSchedulingState.DRAINING, "active", id="activate"),
+            pytest.param({"is_paused": True}, DagSchedulingState.DRAINING, "paused", id="is_paused-clears"),
+            pytest.param({"is_paused": False}, DagSchedulingState.DRAINING, "active", id="unpause-clears"),
+        ],
+    )
+    def test_patch_dags_sets_both_flags(self, test_client, session, body, initial_state, expected_state):
+        for dag_id in (DAG1_ID, DAG2_ID):
+            session.get(DagModel, dag_id).set_scheduling_state(initial_state)
+        session.commit()
+
+        response = test_client.patch("/dags", json=body, params={"dag_id_pattern": "~"})
+
+        assert response.status_code == 200
+        assert {dag["scheduling_state"] for dag in response.json()["dags"]} == {expected_state}
+        session.expire_all()
+        for dag_id in (DAG1_ID, DAG2_ID):
+            assert session.get(DagModel, dag_id).scheduling_state == expected_state
+
     @pytest.mark.parametrize(
         ("query_params", "body", "expected_status_code", "expected_ids", "expected_paused_ids"),
         [
@@ -1268,6 +1389,7 @@ class TestDagDetails(TestDagEndpoint):
             "is_backfillable": False,
             "is_favorite": False,
             "is_paused": False,
+            "scheduling_state": "active",
             "is_paused_upon_creation": None,
             "is_stale": False,
             "latest_dag_version": {
@@ -1316,6 +1438,15 @@ class TestDagDetails(TestDagEndpoint):
         }
         assert res_json == expected
 
+    def test_dag_details_reflects_draining(self, session, test_client):
+        session.get(DagModel, DAG2_ID).set_scheduling_state(DagSchedulingState.DRAINING)
+        session.commit()
+
+        response = test_client.get(f"/dags/{DAG2_ID}/details")
+
+        assert response.status_code == 200
+        assert response.json()["scheduling_state"] == "draining"
+
     def test_dag_details_should_response_401(self, unauthenticated_test_client):
         response = unauthenticated_test_client.get(f"/dags/{DAG1_ID}/details")
         assert response.status_code == 401
@@ -1493,6 +1624,7 @@ class TestGetDag(TestDagEndpoint):
             "has_task_concurrency_limits": True,
             "is_backfillable": False,
             "is_paused": False,
+            "scheduling_state": "active",
             "is_stale": False,
             "last_expired": None,
             "last_parse_duration": last_parse_duration,
@@ -1514,6 +1646,16 @@ class TestGetDag(TestDagEndpoint):
         }
         assert res_json == expected
 
+    def test_get_dag_reflects_draining(self, session, test_client):
+        session.get(DagModel, DAG2_ID).set_scheduling_state(DagSchedulingState.DRAINING)
+        session.commit()
+
+        response = test_client.get(f"/dags/{DAG2_ID}")
+
+        assert response.status_code == 200
+        assert response.json()["scheduling_state"] == "draining"
+        assert response.json()["is_paused"] is False
+
     def test_get_dag_tags_sorted_alphabetically(self, session, test_client, dag_maker):
         """Test that tags are returned in alphabetical order for a single DAG."""
         dag_id = "test_dag_single_sorted_tags"
diff --git a/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py b/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py
index 33f4a5441d..f48013ce01 100644
--- a/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py
+++ b/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dags.py
@@ -34,7 +34,7 @@ from airflow.models.dag_favorite import DagFavorite
 from airflow.models.hitl import HITLDetail
 from airflow.sdk.timezone import utcnow
 from airflow.utils.session import NEW_SESSION, provide_session
-from airflow.utils.state import DagRunState, TaskInstanceState
+from airflow.utils.state import DagRunState, DagSchedulingState, TaskInstanceState
 from airflow.utils.types import DagRunTriggeredByType, DagRunType
 
 from tests_common.test_utils.asserts import count_queries
@@ -193,6 +193,44 @@ class TestGetDagRuns(TestPublicDagEndpoint):
         assert response.status_code == 200
         assert [dag["dag_id"] for dag in response.json()["dags"]] == [DAG1_ID]
 
+    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
+    @pytest.mark.parametrize(
+        ("query_params", "expected_ids"),
+        [
+            ({"scheduling_state": "active"}, [DAG1_ID]),
+            ({"scheduling_state": "draining"}, [DAG2_ID]),
+            ({"scheduling_state": "paused", "exclude_stale": False}, [DAG3_ID]),
+            ({"paused": False}, [DAG1_ID, DAG2_ID]),
+            ({"paused": True, "exclude_stale": False}, [DAG3_ID]),
+        ],
+    )
+    def test_scheduling_state_filter(self, test_client, session, query_params, expected_ids):
+        session.get(DagModel, DAG2_ID).set_scheduling_state(DagSchedulingState.DRAINING)
+        session.commit()
+
+        response = test_client.get("/dags", params=query_params)
+
+        assert response.status_code == 200
+        assert [dag["dag_id"] for dag in response.json()["dags"]] == expected_ids
+
+    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
+    @pytest.mark.parametrize("state", ["queued", "running"])
+    def test_has_unfinished_runs_ignores_dag_runs_limit(self, test_client, session, state):
+        # The unfinished run is the oldest one, outside the single latest run shown for DAG1.
+        older_run = session.scalar(
+            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == "run_id_1")
+        )
+        older_run.state = DagRunState(state)
+        session.commit()
+
+        response = test_client.get("/dags", params={"dag_runs_limit": 1})
+
+        assert response.status_code == 200
+        dags = {dag["dag_id"]: dag for dag in response.json()["dags"]}
+        assert len(dags[DAG1_ID]["latest_dag_runs"]) == 1
+        assert dags[DAG1_ID]["has_unfinished_runs"] is True
+        assert dags[DAG2_ID]["has_unfinished_runs"] is False
+
     @pytest.fixture
     def setup_hitl_data(self, create_task_instance: TaskInstance, session: Session):
         """Setup HITL test data for parametrized tests."""
diff --git a/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py b/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py
index 6ed4eada4b..0baed6861c 100644
--- a/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py
+++ b/airflow-core/tests/unit/api_fastapi/core_api/routes/ui/test_dashboard.py
@@ -29,7 +29,7 @@ from airflow.models.dag import DagModel
 from airflow.models.dagbag import DBDagBag
 from airflow.models.dagrun import DagRun
 from airflow.providers.standard.operators.empty import EmptyOperator
-from airflow.utils.state import DagRunState, TaskInstanceState
+from airflow.utils.state import DagRunState, DagSchedulingState, TaskInstanceState
 from airflow.utils.types import DagRunType
 
 from tests_common.test_utils.asserts import assert_queries_count
@@ -392,6 +392,21 @@ class TestDagStatsEndpoint:
             "queued_dag_count": 1,
         }
 
+    @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_dag_runs")
+    def test_draining_dag_is_not_counted_as_active(self, test_client, session):
+        session.get(DagModel, "test_dag_id").set_scheduling_state(DagSchedulingState.DRAINING)
+        session.commit()
+
+        response = test_client.get("/dashboard/dag_stats")
+
+        assert response.status_code == 200
+        assert response.json() == {
+            "active_dag_count": 0,
+            "failed_dag_count": 0,
+            "running_dag_count": 1,
+            "queued_dag_count": 1,
+        }
+
     @pytest.mark.usefixtures("freeze_time_for_dagruns", "make_failed_dag_runs")
     def test_should_response_200_failed_dag(self, test_client):
         with assert_queries_count(3):
diff --git a/airflow-core/tests/unit/assets/test_manager.py b/airflow-core/tests/unit/assets/test_manager.py
index 3d9d75ff12..c5485bbda2 100644
--- a/airflow-core/tests/unit/assets/test_manager.py
+++ b/airflow-core/tests/unit/assets/test_manager.py
@@ -25,7 +25,7 @@ from typing import TYPE_CHECKING
 from unittest import mock
 
 import pytest
-from sqlalchemy import func, select
+from sqlalchemy import delete, func, select
 from sqlalchemy.dialects import mysql, postgresql, sqlite
 from sqlalchemy.orm import Session
 
@@ -40,7 +40,9 @@ from airflow.models.asset import (
     AssetModel,
     AssetPartitionDagRun,
     DagScheduleAssetAliasReference,
+    DagScheduleAssetNameReference,
     DagScheduleAssetReference,
+    DagScheduleAssetUriReference,
 )
 from airflow.models.dag import DAG, DagModel
 from airflow.models.dagbundle import DagBundleModel
@@ -52,6 +54,7 @@ from airflow.partition_mappers.window import WeekWindow
 from airflow.providers.standard.operators.empty import EmptyOperator
 from airflow.sdk.definitions.asset import Asset
 from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
+from airflow.utils.state import DagSchedulingState
 
 from tests_common.test_utils.config import conf_vars
 from tests_common.test_utils.db import (
@@ -190,6 +193,53 @@ class TestAssetManager:
         )
         assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 2
 
+    @pytest.mark.parametrize("via", ["asset", "alias", "name_ref", "uri_ref"])
+    def test_register_asset_change_skips_draining_dag(self, session, mock_task_instance, testing_dag_bundle, via):
+        session.execute(delete(DagModel).where(DagModel.dag_id.like("mgr_%_consumer")))
+        active_dag = DagModel(dag_id="mgr_active_consumer", bundle_name="testing", is_stale=False)
+        draining_dag = DagModel(dag_id="mgr_draining_consumer", bundle_name="testing", is_stale=False)
+        draining_dag.set_scheduling_state(DagSchedulingState.DRAINING)
+        session.add_all([active_dag, draining_dag])
+
+        asm = AssetModel(uri="test://asset1/", name="test_asset_uri", group="asset")
+        session.add(asm)
+        alias_names = []
+        if via == "asset":
+            asm.scheduled_dags = [DagScheduleAssetReference(dag_id=d.dag_id) for d in (active_dag, draining_dag)]
+        elif via == "alias":
+            asam = AssetAliasModel(name="test_alias_name", group="test")
+            session.add(asam)
+            asam.scheduled_dags = [
+                DagScheduleAssetAliasReference(alias_id=asam.id, dag_id=d.dag_id)
+                for d in (active_dag, draining_dag)
+            ]
+            alias_names = ["test_alias_name"]
+        elif via == "name_ref":
+            session.add_all(
+                DagScheduleAssetNameReference(name="test_asset_uri", dag_id=d.dag_id)
+                for d in (active_dag, draining_dag)
+            )
+        else:
+            session.add_all(
+                DagScheduleAssetUriReference(uri="test://asset1/", dag_id=d.dag_id)
+                for d in (active_dag, draining_dag)
+            )
+        session.flush()
+
+        AssetManager().register_asset_change(
+            task_instance=mock_task_instance,
+            asset=Asset(uri="test://asset1", name="test_asset_uri"),
+            source_alias_names=alias_names,
+            session=session,
+        )
+        session.flush()
+
+        assert (
+            session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id))
+            == 1
+        )
+        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["mgr_active_consumer"]
+
     def test_register_asset_change_no_downstreams(self, session, mock_task_instance):
         asset_manager = AssetManager()
 
diff --git a/airflow-core/tests/unit/cli/commands/test_dag_command.py b/airflow-core/tests/unit/cli/commands/test_dag_command.py
index db7bf2d14a..8dff7477eb 100644
--- a/airflow-core/tests/unit/cli/commands/test_dag_command.py
+++ b/airflow-core/tests/unit/cli/commands/tes
... (truncated; 21376 more characters; read the files)
```

## Answer these questions, one at a time, in this order

1. Which acceptance criterion of this task is not met, or is met only in appearance (hard-coded, special-cased, or untested path)?
2. Which relevant case (boundary, error, empty input, interaction with existing options or state) is not covered by any test of this task?
3. Which existing test was weakened: an assertion removed or loosened, a case deleted, a skip added? Look at the modified declared test files above.
4. Which part of the diff could be removed or simplified without losing behaviour the task requires (dead code, duplicated branches, unnecessary abstraction)?

For each question, report only concrete findings: `file`, `line` (in the new version; 0 if the
finding is not tied to a line), the question number as `pass`, a `severity` and a one-sentence
`text`. `blocking` means the task must not be committed as is: a criterion unmet, a relevant case
untested, an existing test weakened, or a change that does not belong to the task. `advisory`
means worth a comment but not worth another attempt. No finding is a valid answer to a question.

A `blocking` finding is a claim that some test would fail, so it carries that test as `proof`,
and you have run it with `aih check --probe` and seen it fail for the reason you claim before
reporting it (the harness runs it again; a proof that passes for the harness downgrades the
finding, whatever you saw). The proof is:
`path`, a NEW file in the directory of one of the task's test files, named like a test file of
this repository (same package for Go, `test_*.py` for pytest), and `content`, the whole file
(package clause, imports, one or more tests) asserting exactly what you claim. The harness writes
it, runs it with the task's test command and removes it. If it fails, your finding stands and the
implementer gets the test to make pass. If it passes, or does not compile, or you give none, the
finding is recorded as advisory with that reason, not as blocking. A claim you cannot turn into a
test here (behaviour on another platform, style, taste) is advisory from the start: say it, do not
block on it. The tests already pass, so a proof that merely repeats them will pass too.

## Coverage map and probes

Then fill `coverage` with one entry per acceptance criterion marked `[behavior]` above. For
each, name the single test of this task's test files whose assertion proves the criterion:
`test` as a pytest node id (`tests/path.py::test_name`), `line` of the decisive assertion,
`status` `covered` when that assertion would fail if the criterion were broken, `partial` when
a test touches the behaviour but would not fail (asserts only the happy path, or a weaker
property), `uncovered` when no test exercises it. Read the test body: do not infer coverage
from a test's name. `partial` and `uncovered` are blocking, so when you mark one, say in
`note` what test would prove it.

Each entry also carries a `probe`: a test YOU wrote for that criterion and ran with
`aih check --probe`. Approving is a claim that the behaviour holds, and the task's own tests
were written by the session that wrote the code, so they prove what it believed, not what the
criterion says. Write the probe from the criterion's text, through the surface the request names
(the program's configuration, its public API, a real client), exercising the case the criterion
is about rather than the happy path; do not copy an existing test. Run it until it runs (with
`--run <your test names>` where supported: the rest of the package is not what you are checking;
a run that executed zero tests, `[no tests to run]`, counts as not run);
a probe that passes verifies the criterion, a probe that fails is a blocking finding of pass 1 with
that probe as `proof`. The harness runs every probe again, and runs every passing probe once more
on the base commit without the production change: a probe that passes there too asserts nothing
this change adds and does not count (a test with no assertion, or one that only checks what
already held). A criterion whose probe is missing, does not run, or does not discriminate is
recorded as unprobed, visible in the pull request, and you will be asked for it once.
`probe.path` is a new file in a directory of the task's test files, named like a test file of the
repository; `probe.content` is the whole file.

`verdict` is `request_changes` if and only if there is at least one `blocking` finding or a
criterion that is not `covered`.
