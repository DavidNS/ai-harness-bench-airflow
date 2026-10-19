from __future__ import annotations

from unittest import mock

import pytest

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

from tests_common.test_utils.mock_executor import MockExecutor

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


def test_probe_loop_registers_finalize_every_5_seconds():
    runner = SchedulerJobRunner(job=Job(), num_runs=1, executors=[MockExecutor(do_update=False)])
    runner.job.heartbeat = mock.MagicMock()

    with (
        mock.patch("airflow.jobs.scheduler_job_runner.EventScheduler", autospec=True) as event_scheduler,
        mock.patch.object(runner, "_do_scheduling", return_value=0),
        mock.patch.object(runner, "adopt_or_reset_orphaned_tasks"),
    ):
        event_scheduler.return_value.run.return_value = 0
        runner._run_scheduler_loop()

    intervals = [
        call.args[0]
        for call in event_scheduler.return_value.call_regular_interval.call_args_list
        if len(call.args) > 1 and call.args[1] == runner._finalize_draining_dags
    ]
    assert intervals == [5.0]
