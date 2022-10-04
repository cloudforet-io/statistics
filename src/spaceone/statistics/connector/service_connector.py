import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *
from spaceone.statistics.error.resource import *

__all__ = ['ServiceConnector']

_LOGGER = logging.getLogger(__name__)


class ServiceConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self.client = {}

    def _init_client(self, service, resource):
        if service not in self.client:
            if service not in self.config:
                raise ERROR_INVALID_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

            e = parse_grpc_endpoint(self.config[service])
            self.client[service] = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'],
                                                 max_message_length=1024*1024*256)

    def _check_resource_type(self, service, resource):
        if service not in self.client.keys():
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

        if not hasattr(self.client[service], resource):
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

        if not hasattr(getattr(self.client[service], resource), 'stat'):
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=f'{service}.{resource}')

    def stat_resource(self, service, resource, query, domain_id):
        _LOGGER.debug(f'[stat_resource] {service}.{resource} : {query}')

        self._init_client(service, resource)
        self._check_resource_type(service, resource)

        response = getattr(self.client[service], resource).stat({
            'domain_id': domain_id,
            'query': query
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
