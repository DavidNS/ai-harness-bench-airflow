from __future__ import annotations

import json
import os

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_command
from airflow.models.dag import DagModel
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, parse_and_sync_to_db

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def restore():
    clear_db_runs()
    clear_db_dags()
    with conf_vars({("core", "load_examples"): "True"}):
        parse_and_sync_to_db(os.devnull)
    yield
    clear_db_runs()
    clear_db_dags()


@pytest.mark.parametrize(
    ("cmd", "func", "expected"),
    [("pause", dag_command.dag_pause, "paused"), ("unpause", dag_command.dag_unpause, "active")],
)
def test_probe_cli(cmd, func, expected):
    parser = cli_parser.get_parser()
    with create_session() as session:
        session.get(DagModel, "example_bash_operator").set_scheduling_state("draining")
    func(parser.parse_args(["dags", cmd, "example_bash_operator", "--yes"]))
    m = DagModel.get_dagmodel("example_bash_operator")
    assert (m.scheduling_state, m.is_draining) == (expected, False)


def test_probe_details(capsys):
    parser = cli_parser.get_parser()
    with create_session() as session:
        session.get(DagModel, "example_bash_operator").set_scheduling_state("draining")
    dag_command.dag_details(
        parser.parse_args(["dags", "details", "example_bash_operator", "--output", "json"])
    )
    assert json.loads(capsys.readouterr().out)[0]["scheduling_state"] == "draining"
