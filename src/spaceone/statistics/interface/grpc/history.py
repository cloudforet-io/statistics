from spaceone.api.statistics.v1 import history_pb2, history_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class History(BaseAPI, history_pb2_grpc.HistoryServicer):

    pb2 = history_pb2
    pb2_grpc = history_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('HistoryService', metadata) as history_service:
            history_service.create(params)
            return self.locator.get_info('EmptyInfo')

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('HistoryService', metadata) as history_service:
            history_vos, total_count = history_service.list(params)
            return self.locator.get_info('HistoryInfo', history_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('HistoryService', metadata) as history_service:
            return self.locator.get_info('StatisticsInfo', history_service.stat(params))
