from __future__ import annotations

from unittest import mock

import pendulum
import pytest
from sqlalchemy import delete, select

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.asset import AssetEvent, AssetPartitionDagRun, PartitionedAssetKeyLog
from airflow.models.backfill import Backfill, BackfillDagRun, ReprocessBehavior
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, IdentityMapper
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
from airflow.timetables.simple import PartitionedAtRuntime
from airflow.utils.state import DagRunState, DagSchedulingState, State
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_apdr, clear_db_backfills, clear_db_pakl
from tests_common.test_utils.mock_executor import MockExecutor

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


def _runner():
    return SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )


def _make_dag(dag_maker, session, dag_id, state, run_state=None):
    with dag_maker(dag_id=dag_id, schedule="@daily", session=session):
        EmptyOperator(task_id="t")
    run = dag_maker.create_dagrun(state=run_state) if run_state else None
    session.get(DagModel, dag_id).set_scheduling_state(state)
    session.flush()
    return run


@pytest.mark.need_serialized_dag
def test_probe_partitioned_event_ignored_while_draining(dag_maker, session):
    clear_db_apdr()
    clear_db_pakl()
    asset = Asset(name="probe-asset")
    with dag_maker(
        dag_id="probe-consumer",
        schedule=PartitionedAssetTimetable(assets=asset, default_partition_mapper=IdentityMapper()),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    with dag_maker(dag_id="probe-producer", schedule=PartitionedAtRuntime(), session=session) as dag:
        EmptyOperator(task_id="hi", outlets=[asset])
    dr = dag_maker.create_dagrun(partition_key="2026-05-20T01:00:00", session=session)
    [ti] = dr.get_task_instances(session=session)
    session.commit()
    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[o.asprofile() for o in dag.get_task("hi").outlets],
        outlet_events=[],
        session=session,
    )
    session.commit()
    event = session.scalar(select(AssetEvent).where(AssetEvent.source_dag_id == "probe-producer"))
    apdr = session.scalar(
        select(AssetPartitionDagRun)
        .join(
            PartitionedAssetKeyLog,
            PartitionedAssetKeyLog.asset_partition_dag_run_id == AssetPartitionDagRun.id,
        )
        .where(PartitionedAssetKeyLog.asset_event_id == event.id)
    )

    session.get(DagModel, "probe-consumer").set_scheduling_state(DagSchedulingState.DRAINING)
    session.commit()
    assert _runner()._create_dagruns_for_partitioned_asset_dags(session=session) == set()
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert session.scalars(select(DagRun).where(DagRun.dag_id == "probe-consumer")).all() == []

    session.get(DagModel, "probe-consumer").set_scheduling_state(DagSchedulingState.ACTIVE)
    session.commit()
    assert _runner()._create_dagruns_for_partitioned_asset_dags(session=session) == {"probe-consumer"}


@pytest.mark.parametrize(
    ("state", "expected_queued"),
    [(DagSchedulingState.DRAINING, True), (DagSchedulingState.ACTIVE, True), (DagSchedulingState.PAUSED, False)],
)
def test_probe_tis_queued_by_scheduling_state(dag_maker, session, state, expected_queued):
    dag_id = f"probe_queue_{state.value}"
    dr = _make_dag(dag_maker, session, dag_id, state, DagRunState.RUNNING)
    (ti,) = dr.task_instances
    ti.state = State.SCHEDULED
    session.flush()

    res = _runner()._executable_task_instances_to_queued(max_tis=32, session=session)

    assert ([queued.key for queued in res] == [ti.key]) is expected_queued
    session.rollback()


def test_probe_manual_run_can_be_created_for_draining_dag(dag_maker, session):
    _make_dag(dag_maker, session, "probe_manual", DagSchedulingState.DRAINING)
    dr = dag_maker.create_dagrun(
        run_id="manual_run", run_type=DagRunType.MANUAL, state=DagRunState.RUNNING, session=session
    )
    assert dr.run_type == DagRunType.MANUAL
    assert session.get(DagModel, "probe_manual").scheduling_state == DagSchedulingState.DRAINING


def test_probe_finalize_pauses_idle_and_logs(dag_maker, session):
    clear_db_backfills()
    session.execute(delete(Log))
    _make_dag(dag_maker, session, "probe_idle", DagSchedulingState.DRAINING)
    _make_dag(dag_maker, session, "probe_done", DagSchedulingState.DRAINING, DagRunState.FAILED)
    _make_dag(dag_maker, session, "probe_active", DagSchedulingState.ACTIVE)
    _make_dag(dag_maker, session, "probe_paused", DagSchedulingState.PAUSED)

    _runner()._finalize_draining_dags(session)
    session.flush()
    session.expire_all()

    for dag_id in ("probe_idle", "probe_done", "probe_paused"):
        dag = session.get(DagModel, dag_id)
        assert (dag.is_paused, dag.is_draining) == (True, False)
    assert session.get(DagModel, "probe_active").scheduling_state == DagSchedulingState.ACTIVE
    logs = session.scalars(select(Log).where(Log.event == "drain_completed").order_by(Log.dag_id)).all()
    assert [(log.dag_id, log.owner) for log in logs] == [("probe_done", "scheduler"), ("probe_idle", "scheduler")]


@pytest.mark.parametrize("run_state", [DagRunState.QUEUED, DagRunState.RUNNING])
def test_probe_finalize_keeps_dags_with_unfinished_runs(dag_maker, session, run_state):
    clear_db_backfills()
    session.execute(delete(Log))
    _make_dag(dag_maker, session, "probe_busy", DagSchedulingState.DRAINING, run_state)

    _runner()._finalize_draining_dags(session)
    session.flush()
    session.expire_all()

    assert session.get(DagModel, "probe_busy").scheduling_state == DagSchedulingState.DRAINING
    assert session.scalars(select(Log).where(Log.event == "drain_completed")).all() == []


def test_probe_finalize_waits_for_backfill_without_runs(dag_maker, session):
    clear_db_backfills()
    _make_dag(dag_maker, session, "probe_bf", DagSchedulingState.DRAINING)
    backfill = Backfill(
        dag_id="probe_bf",
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add(backfill)
    session.flush()

    _runner()._finalize_draining_dags(session)
    session.flush()
    session.expire_all()
    assert session.get(DagModel, "probe_bf").scheduling_state == DagSchedulingState.DRAINING

    dag_run = DagRun(
        dag_id="probe_bf",
        run_id="backfill__2021-01-01T00:00:00+00:00",
        run_type=DagRunType.BACKFILL_JOB,
        logical_date=pendulum.parse("2021-01-01"),
        data_interval=(pendulum.parse("2021-01-01"), pendulum.parse("2021-01-02")),
        run_after=pendulum.parse("2021-01-02"),
        state=DagRunState.RUNNING,
        backfill_id=backfill.id,
    )
    session.add(dag_run)
    session.flush()
    session.add(
        BackfillDagRun(
            backfill_id=backfill.id,
            dag_run_id=dag_run.id,
            logical_date=pendulum.parse("2021-01-01"),
            sort_ordinal=1,
        )
    )
    session.flush()

    _runner()._finalize_draining_dags(session)
    session.flush()
    session.expire_all()
    # a backfill with runs no longer holds the drain open itself; the running run does
    assert session.get(DagModel, "probe_bf").scheduling_state == DagSchedulingState.DRAINING

    session.get(DagRun, dag_run.id).state = DagRunState.SUCCESS
    session.flush()
    _runner()._finalize_draining_dags(session)
    session.flush()
    session.expire_all()
    assert session.get(DagModel, "probe_bf").scheduling_state == DagSchedulingState.PAUSED


def test_probe_finalize_orders_by_dag_id_and_respects_limit(dag_maker, session):
    clear_db_backfills()
    session.execute(delete(DagModel).where(DagModel.is_draining.is_(True)))
    for dag_id in ("probe_z", "probe_x", "probe_y"):
        _make_dag(dag_maker, session, dag_id, DagSchedulingState.DRAINING)

    with mock.patch.object(DagModel, "NUM_DAGS_PER_DAGRUN_QUERY", 2):
        _runner()._finalize_draining_dags(session)
    session.flush()
    session.expire_all()

    assert session.get(DagModel, "probe_x").scheduling_state == DagSchedulingState.PAUSED
    assert session.get(DagModel, "probe_y").scheduling_state == DagSchedulingState.PAUSED
    assert session.get(DagModel, "probe_z").scheduling_state == DagSchedulingState.DRAINING
