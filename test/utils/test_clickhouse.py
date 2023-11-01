import pytest
from shared.utils.clickhouse import ClickHouseDbClient


class MockClickHouseClient:
    def __init__(self, host, port, user):
        self.host = host
        self.port = port
        self.user = user


@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv("CH_HOST", "env_host")
    monkeypatch.setenv("CH_PORT", "env_port")
    monkeypatch.setenv("CH_USER", "env_user")
    monkeypatch.setattr("clickhouse_connect.get_client", MockClickHouseClient)


@pytest.fixture
def db_client():
    return ClickHouseDbClient()


def test_get_client_uses_parameters_over_env_vars(setup_env, db_client):
    client = db_client.get_client(host="param_host", port="param_port", user="param_user")
    assert client.host == "param_host"
    assert client.port == "param_port"
    assert client.user == "param_user"


def test_get_client_uses_environment_variables_when_no_parameters_provided(setup_env, db_client):
    client = db_client.get_client()
    assert client.host == "env_host"
    assert client.port == "env_port"
    assert client.user == "env_user"


def test_get_client_uses_default_values_when_no_parameters_or_env_vars_provided(monkeypatch, db_client):
    monkeypatch.setattr("clickhouse_connect.get_client", MockClickHouseClient)
    client = db_client.get_client()
    assert client.host == ClickHouseDbClient.DEFAULT_HOST
    assert client.port == ClickHouseDbClient.DEFAULT_PORT
    assert client.user == ClickHouseDbClient.DEFAULT_USER
