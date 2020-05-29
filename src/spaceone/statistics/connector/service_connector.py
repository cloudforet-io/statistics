import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *
from spaceone.statistics.error.resource import *

__all__ = ['ServiceConnector']

_LOGGER = logging.getLogger(__name__)


class ServiceConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self._init_client()

    def _init_client(self):
        self.client = {}
        for service, endpoint in self.config.items():
            e = parse_endpoint(endpoint)
            # _LOGGER.debug(f'[_init_client] Load endpoint : {endpoint}')
            if e.get('path') is None:
                raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

            version = e.get('path').replace('/', '')

            self.client[service] = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}', version=version)

    def check_resource_type(self, service, resource):
        if service not in self.client.keys():
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

        if not hasattr(self.client[service], resource):
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

        if not hasattr(getattr(self.client[service], resource), 'stat'):
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

    def stat_resource(self, service, resource, query, domain_id):
        _LOGGER.debug(f'[stat_resource] {service}.{resource} : {query}')

        response = getattr(self.client[service], resource).stat({
            'domain_id': domain_id,
            'query': query
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
