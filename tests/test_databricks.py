from databricks.sql.client import Cursor
import pytest
from os import getenv
from lambda_databricks.credentials.helpers import HelperCredentialsAWS
from lambda_databricks.credentials.secrets.databricks import SecretsIds, Secrets
from lambda_databricks.clients.databricks import SQL, PySpark


@pytest.fixture
def secrets_helper() -> HelperCredentialsAWS:
    ids = SecretsIds(
        databricks_access_token=getenv("AWS_SECRET_NAME_DATABRICKS_ACCESS_TOKEN")
    )
    return HelperCredentialsAWS(
        credentials_ids=ids, region_name=getenv("AWS_SECRET_REGION_NAME")
    )


@pytest.fixture
def secrets(secrets_helper: HelperCredentialsAWS) -> Secrets:
    return secrets_helper.get_credentials()


@pytest.fixture
def databricks_cursor(secrets: Secrets) -> Cursor:
    cursor = SQL(
        server_hostname=getenv("DATABRICKS_SERVER_HOST"),
        http_path=getenv("DATABRICKS_HTTP_PATH"),
        access_token=secrets.databricks_access_token,
    ).cursor
    yield cursor
    cursor.close()


@pytest.fixture
def pyspark_client(secrets: Secrets) -> PySpark:
    return PySpark(
        spark_master_host=getenv("DATABRICKS_SPARK_HOST"),
        server_hostname=getenv("DATABRICKS_SERVER_HOST"),
        http_path=getenv("DATABRICKS_HTTP_PATH"),
        access_token=secrets.databricks_access_token,
    )


@pytest.fixture
def table_name() -> str:
    return "dl_external_dev.banana"


def test_secrets_get(secrets: Secrets):
    for key, value in secrets.__dict__.items():
        assert value is not None, f"'{key}' is None"


def test_read_connection_databricks(databricks_cursor: Cursor, table_name: str):
    databricks_cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
    result = databricks_cursor.fetchall()
    assert result is not None


def test_write_connection_databricks(databricks_cursor: Cursor, table_name: str):
    count_query = f"SELECT COUNT(*) rows FROM {table_name}"
    databricks_cursor.execute(count_query)
    before_count = databricks_cursor.fetchone()[0]

    databricks_cursor.execute(
        f"INSERT INTO {table_name} VALUES ({before_count}, 'tar')"
    )

    databricks_cursor.execute(count_query)
    after_count = databricks_cursor.fetchone()[0]

    assert after_count > before_count


def test_pyspark(pyspark_client: PySpark, table_name: str):
    df = pyspark_client.read_table(table_name)
    assert df is not None
