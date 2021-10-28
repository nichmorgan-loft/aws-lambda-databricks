from dataclasses import dataclass
from ..interfaces.credentials import IHelperCredentials
from .secrets.databricks import SecretsIds, Secrets
from botocore.exceptions import ClientError
import boto3
import logging
import json
import base64


@dataclass
class HelperCredentialsAWS(IHelperCredentials):
    credentials_ids: SecretsIds
    region_name: str

    def _get_credential(self, credential_id: str) -> str:
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager", region_name=self.region_name
            )
            get_secret_value_response = client.get_secret_value(SecretId=credential_id)

        except ClientError as e:
            logging.error("Get AWS credentials fails.")
            raise e

        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if "SecretString" in get_secret_value_response:
                secret_json = get_secret_value_response["SecretString"]
            else:
                secret_json = base64.b64decode(
                    get_secret_value_response["SecretBinary"]
                ).decode("utf-8")

            secret = json.loads(secret_json)
            return list(secret.values()).pop()

    def get_credentials(self) -> Secrets:
        secrets = {}
        for name, id in self.credentials_ids.__dict__.items():
            secrets[name] = self._get_credential(id)
        return Secrets(**secrets)
