from __future__ import annotations

import pytest
from sqlalchemy import func, select

from airflow.assets.manager import AssetManager
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
)
from airflow.models.dag import DagModel
from airflow.sdk.definitions.asset import Asset
from airflow.utils.state import DagSchedulingState

from tests_common.test_utils.db import clear_db_assets, clear_db_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


@pytest.fixture(autouse=True)
def clean():
    clear_db_assets()
    clear_db_dags()
    yield
    clear_db_assets()
    clear_db_dags()


@pytest.mark.parametrize("only_draining", [False, True])
@pytest.mark.parametrize("via", ["asset", "alias", "name_ref", "uri_ref"])
def test_probe_draining_dag_not_queued(session, testing_dag_bundle, via, only_draining):
    dags = []
    if not only_draining:
        dags.append(DagModel(dag_id="probe_mgr_active", bundle_name="testing", is_stale=False))
    draining = DagModel(dag_id="probe_mgr_draining", bundle_name="testing", is_stale=False)
    draining.set_scheduling_state(DagSchedulingState.DRAINING)
    dags.append(draining)
    session.add_all(dags)

    asm = AssetModel(uri="test://probe/", name="probe_asset", group="asset")
    session.add(asm)
    alias_names = []
    if via == "asset":
        asm.scheduled_dags = [DagScheduleAssetReference(dag_id=d.dag_id) for d in dags]
    elif via == "alias":
        alias = AssetAliasModel(name="probe_alias", group="test")
        session.add(alias)
        alias.scheduled_dags = [DagScheduleAssetAliasReference(alias_id=alias.id, dag_id=d.dag_id) for d in dags]
        alias_names = ["probe_alias"]
    elif via == "name_ref":
        session.add_all(DagScheduleAssetNameReference(name="probe_asset", dag_id=d.dag_id) for d in dags)
    else:
        session.add_all(DagScheduleAssetUriReference(uri="test://probe/", dag_id=d.dag_id) for d in dags)
    session.flush()

    AssetManager().register_asset_change(
        task_instance=None,
        asset=Asset(uri="test://probe", name="probe_asset"),
        source_alias_names=alias_names,
        session=session,
    )
    session.flush()

    assert session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id)) == 1
    queued = set(session.scalars(select(AssetDagRunQueue.target_dag_id)))
    assert queued == (set() if only_draining else {"probe_mgr_active"})
