Your review is incomplete.

These behaviour criteria have no probe that counts: a probe that runs zero tests does not count (check the output names your test), and neither does one that passes without the change (the harness runs every passing probe on the base commit too; a test that asserts nothing, or only what already held, proves nothing about this change). For each one, write a NEW `aih_probe*` test that asserts the criterion from the outside and would fail if the change were reverted, run it with `aih check --probe <file> --run <its test names>`, fix it until it runs, and put it as `probe` (path named like a test file in a directory of the task's test files, full content) in that criterion's coverage entry. If a probe fails, that is a blocking finding with the probe as `proof`.
- ACC19: DagSchedulingState has exactly members/values active, draining, paused (test_state.py). (no probe test was given)
- ACC20: DagModel.scheduling_state derives active/draining/paused from flags; set_scheduling_state sets both flags consistently; is_draining defaults to false; the DB rejects is_paused and is_draining both true. (no probe test was given)
- ACC22: A draining Dag is not returned by dags_needing_dagruns for scheduled runs, and no run is created for its pending partitioned-asset events. (no probe test was given)
- ACC23: AssetManager records the asset event but does not queue a draining Dag scheduled directly, via alias, or by name/URI; active Dags still queue. (no probe test was given)
- ACC24: Task instances of queued/running runs of a draining Dag are still scheduled and queued, and its unfinished runs are still examined; manual triggers still create runs. (no probe test was given)
- ACC25: _finalize_draining_dags moves a draining Dag with no queued/running run and no backfill without Dag runs to paused, clears draining and logs drain_completed with owner scheduler. (no probe test was given)
- ACC26: _finalize_draining_dags leaves a Dag draining when it has a queued or running run, or a backfill row with no Dag runs; processes in dag_id order with limit and row locks. (no probe test was given)
- ACC27: The scheduler loop registers _finalize_draining_dags with a 5 second interval. (no probe test was given)
- ACC28: Auto-pause after consecutive failed runs sets is_paused true and is_draining false for a draining Dag. (no probe test was given)
- ACC29: DAGResponse/DAGDetailsResponse expose scheduling_state defaulting to active; GET reflects draining. (no probe test was given)
- ACC30: PATCH /dags/{dag_id} and PATCH /dags accept exactly one non-null of is_paused/scheduling_state (explicit null for the other accepted); both, neither, or a null-only field yield 422. (no probe test was given)
- ACC31: update_mask naming is_paused or scheduling_state works when matching the body; a mask naming the other field is 422; response reflects new state; bulk PATCH sets both flags; is_paused true/false clears draining. (no probe test was given)
- ACC32: /ui/dags scheduling_state filter returns only matching Dags; paused=false includes draining; paused filter otherwise unchanged. (no probe test was given)
- ACC33: Every Dag in /ui/dags has has_unfinished_runs true iff a queued/running run exists, regardless of dag_runs_limit. (no probe test was given)
- ACC34: Dashboard active_dag_count excludes draining Dags. (no probe test was given)
- ACC35: airflow dags pause sets paused and clears draining; unpause sets active and clears draining; a draining Dag counts as changed by both; dags details output includes scheduling_state. (no probe test was given)

These coverage entries cannot be checked as written. `test` is one pytest-style node id, `path::test_name`, naming a test function that exists in one of the task's test files; `line` is its decisive assertion. Fix each entry (or mark the criterion `partial`/`uncovered` and say what test would prove it).
- ACC19 is not proven by a test: `airflow-core/tests/unit/utils/test_state.py::test_dag_scheduling_state` does not exist in airflow-core/tests/unit/utils/test_state.py
- ACC22 is not proven by a test: `airflow-core/tests/unit/jobs/test_scheduler_job.py::test_draining_dag_not_in_dags_needing_dagruns` does not exist in airflow-core/tests/unit/jobs/test_scheduler_job.py
- ACC24 is not proven by a test: `airflow-core/tests/unit/jobs/test_scheduler_job.py::test_draining_queues_tis` does not exist in airflow-core/tests/unit/jobs/test_scheduler_job.py
- ACC28 is not proven by a test: `airflow-core/tests/unit/models/test_dagrun.py::test_auto_pause_clears_draining` does not exist in airflow-core/tests/unit/models/test_dagrun.py

Then return the whole review JSON again, with every entry.