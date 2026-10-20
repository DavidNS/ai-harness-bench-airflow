from __future__ import annotations

import pytest
from sqlalchemy import select

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag import DagModel
from airflow.models.log import Log
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def _clean():
    clear_db_runs()
    clear_db_dags()
    yield
    clear_db_runs()
    clear_db_dags()


def test_probe_finalize(dag_maker, session):
    for dag_id, run_state in (("probe_busy", DagRunState.RUNNING), ("probe_done", DagRunState.SUCCESS)):
        with dag_maker(dag_id=dag_id, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=run_state)
        session.get(DagModel, dag_id).set_scheduling_state("draining")
    session.flush()
    runner = SchedulerJobRunner(Job())
    runner._finalize_draining_dags(session)
    session.flush()
    assert session.get(DagModel, "probe_busy").scheduling_state == "draining"
    assert session.get(DagModel, "probe_done").scheduling_state == "paused"
    logs = session.scalars(select(Log).where(Log.event == "drain_completed")).all()
    assert [(x.dag_id, x.owner) for x in logs] == [("probe_done", "scheduler")]


def test_probe_enqueue_draining(dag_maker, session):
    with dag_maker(dag_id="probe_enq", session=session):
        EmptyOperator(task_id="t")
    runner = SchedulerJobRunner(Job())
    dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=DagRunState.RUNNING)
    (ti,) = dr.task_instances
    ti.state = TaskInstanceState.SCHEDULED
    session.get(DagModel, "probe_enq").set_scheduling_state("draining")
    session.flush()
    runner._critical_section_enqueue_task_instances(session)
    session.flush()
    assert dr.get_task_instance("t", session=session).state == TaskInstanceState.QUEUED
