from spaceone.api.statistics.v1 import resource_pb2, resource_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Resource(BaseAPI, resource_pb2_grpc.ResourceServicer):

    pb2 = resource_pb2
    pb2_grpc = resource_pb2_grpc

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        with self.locator.get_service('ResourceService', metadata) as resource_service:
            return self.locator.get_info('StatisticsInfo', resource_service.stat(params))
