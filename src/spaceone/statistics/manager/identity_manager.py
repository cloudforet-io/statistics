import logging
from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="identity"
        )

    def list_domains(self, query: dict) -> dict:
        system_token = config.get_global("TOKEN")
        return self.identity_conn.dispatch(
            "Domain.list", {"query": query}, token=system_token
        )
