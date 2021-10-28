from dataclasses import dataclass
from typing import Any
from databricks.sql.client import Connection, Cursor
from databricks import sql
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


@dataclass
class IClient(ABC):
    server_hostname: str
    http_path: str
    access_token: str

    @property
    @abstractmethod
    def connection(self) -> Any:
        pass


@dataclass
class SQL(IClient):
    @property
    def connection(self) -> Connection:
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        )

    @property
    def cursor(self) -> Cursor:
        return self.connection.cursor()


@dataclass
class PySpark(IClient):
    spark_master_host: str
    port: int = 433
    spark_port: int = 37685
    app_name: str = "PySparkApp"

    @property
    def connection_url(self) -> str:
        return f"spark://{self.spark_master_host}:{self.spark_port}"

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:spark://{self.server_hostname}:{self.port}/default;transportMode=http;ssl=1;"
            f"httpPath={self.http_path};AuthMech=3;UID=token;"
            f"PWD={self.access_token}"
        )

    @property
    def connection(self) -> SparkSession:
        return (
            SparkSession.builder.master(self.connection_url)
            .appName(self.app_name)
            .getOrCreate()
        )

    def read_table(self, table_name: str, **kwargs) -> DataFrame:
        return self.connection.read.jdbc(self.jdbc_url, table_name, **kwargs)
