from __future__ import annotations

from unittest import mock

import pytest
from sqlalchemy import func, select

from airflow.assets.manager import AssetManager
from airflow.models.asset import (
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
)
from airflow.models.dag import DagModel
from airflow.sdk.definitions.asset import Asset

from tests_common.test_utils.db import clear_db_assets, clear_db_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def _clean():
    clear_db_assets()
    clear_db_dags()
    yield
    clear_db_assets()
    clear_db_dags()


def test_probe_draining_not_queued(session, testing_dag_bundle):
    for dag_id, state in (("probe_active", "active"), ("probe_draining", "draining")):
        m = DagModel(dag_id=dag_id, bundle_name="testing", is_stale=False)
        m.set_scheduling_state(state)
        session.add(m)
    asm = AssetModel(uri="test://probe/", name="probe_asset", group="asset")
    session.add(asm)
    session.flush()
    for dag_id in ("probe_active", "probe_draining"):
        asm.scheduled_dags.append(DagScheduleAssetReference(dag_id=dag_id))
    session.flush()
    ti = mock.MagicMock()
    ti.dag_id = "producer"
    ti.task_id = "t"
    ti.run_id = "r"
    ti.map_index = -1
    AssetManager().register_asset_change(
        task_instance=ti, asset=Asset(uri="test://probe", name="probe_asset"), session=session
    )
    session.flush()
    assert session.scalar(select(func.count()).select_from(AssetEvent)) == 1
    assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == ["probe_active"]
