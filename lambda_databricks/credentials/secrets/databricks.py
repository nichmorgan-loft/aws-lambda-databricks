from dataclasses import dataclass
from lambda_databricks.interfaces.credentials import (
    ISecrets,
    ISecretsIds,
)


@dataclass
class SecretsIds(ISecretsIds):
    databricks_access_token: str


@dataclass
class Secrets(ISecrets):
    databricks_access_token: str
