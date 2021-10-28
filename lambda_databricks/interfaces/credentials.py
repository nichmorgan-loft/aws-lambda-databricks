from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any


@dataclass
class ISecrets(ABC):
    pass


@dataclass
class ISecretsIds(ABC):
    pass


@dataclass
class IHelperCredentials(ABC):
    credentials_ids: ISecretsIds

    @abstractmethod
    def _get_credential(self, credential_id: str) -> Any:
        """Get one credential by credential id.

        Args:
            credential_id (str): Credential id.

        Returns:
            Any: The secret value.
        """

    @abstractmethod
    def get_credentials(self) -> ISecrets:
        """Get all credentials"""
