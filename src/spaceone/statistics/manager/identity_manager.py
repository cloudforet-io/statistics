import logging
from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="identity", token=config.get_global("TOKEN")
        )

    def list_domains(self, query: dict) -> dict:
        return self.identity_conn.dispatch("Domain.list", {"query": query})
