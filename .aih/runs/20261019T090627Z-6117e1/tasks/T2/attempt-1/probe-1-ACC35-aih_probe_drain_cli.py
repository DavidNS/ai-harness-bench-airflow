from __future__ import annotations

import json

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_command
from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagSchedulingState

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

IDS = ("probe_cli_a", "probe_cli_b", "probe_cli_c")


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


@pytest.fixture(autouse=True)
def cli_dags(dag_maker, session):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    for dag_id in IDS:
        with dag_maker(dag_id, schedule=None, serialized=True):
            EmptyOperator(task_id="t")
    session.commit()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


def _flags(session, dag_id):
    session.expire_all()
    dag = session.get(DagModel, dag_id)
    return dag.is_paused, dag.is_draining


@pytest.mark.parametrize(
    ("verb", "initial", "expected"),
    [
        ("pause", DagSchedulingState.DRAINING, (True, False)),
        ("pause", DagSchedulingState.ACTIVE, (True, False)),
        ("unpause", DagSchedulingState.DRAINING, (False, False)),
        ("unpause", DagSchedulingState.PAUSED, (False, False)),
    ],
)
def test_probe_pause_unpause_sets_both_flags(session, capsys, verb, initial, expected):
    session.get(DagModel, "probe_cli_a").set_scheduling_state(initial)
    session.commit()

    getattr(dag_command, f"dag_{verb}")(cli_parser.get_parser().parse_args(["dags", verb, "probe_cli_a"]))

    out = capsys.readouterr().out
    assert "probe_cli_a" in out
    assert "were found" not in out
    assert _flags(session, "probe_cli_a") == expected


def test_probe_unpause_of_active_and_pause_of_paused_change_nothing(session, capsys):
    session.get(DagModel, "probe_cli_b").set_scheduling_state(DagSchedulingState.PAUSED)
    session.commit()

    dag_command.dag_pause(cli_parser.get_parser().parse_args(["dags", "pause", "probe_cli_b"]))
    assert "were found" in capsys.readouterr().out
    dag_command.dag_unpause(cli_parser.get_parser().parse_args(["dags", "unpause", "probe_cli_c"]))
    assert "were found" in capsys.readouterr().out


def test_probe_regex_pause_catches_draining_dags(session):
    session.get(DagModel, "probe_cli_a").set_scheduling_state(DagSchedulingState.DRAINING)
    session.get(DagModel, "probe_cli_b").set_scheduling_state(DagSchedulingState.PAUSED)
    session.commit()

    dag_command.dag_unpause(
        cli_parser.get_parser().parse_args(["dags", "unpause", "probe_cli_.*", "--treat-dag-id-as-regex", "--yes"])
    )

    for dag_id in IDS:
        assert _flags(session, dag_id) == (False, False)


def test_probe_details_include_scheduling_state(session, capsys):
    session.get(DagModel, "probe_cli_a").set_scheduling_state(DagSchedulingState.DRAINING)
    session.commit()

    dag_command.dag_details(
        cli_parser.get_parser().parse_args(["dags", "details", "probe_cli_a", "--output", "json"])
    )

    assert json.loads(capsys.readouterr().out)[0]["scheduling_state"] == "draining"
