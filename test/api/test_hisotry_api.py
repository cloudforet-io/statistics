import unittest
from unittest.mock import patch
from mongoengine import connect, disconnect
from google.protobuf.json_format import MessageToDict
from google.protobuf.empty_pb2 import Empty

from spaceone.core.unittest.result import print_message
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.service import BaseService
from spaceone.core.locator import Locator
from spaceone.core.pygrpc import BaseAPI
from spaceone.api.statistics.v1 import history_pb2
from spaceone.statistics.api.v1.history import History
from test.factory.history_factory import HistoryFactory


class _MockHistoryService(BaseService):

    def list(self, params):
        return HistoryFactory.build_batch(10, **params), 10

    def stat(self, params):
        return {
            'results': [{'project_id': utils.generate_id('project'), 'server_count': 100}]
        }


class TestHistoryAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(service='statistics')
        connect('test', host='mongomock://localhost')
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockHistoryService())
    @patch.object(BaseAPI, 'parse_request')
    def test_list_history(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        history_servicer = History()
        history_info = history_servicer.list({}, {})

        print_message(history_info, 'test_list_history')

        self.assertIsInstance(history_info, history_pb2.HistoryInfo)
        self.assertIsInstance(history_info.results[0], history_pb2.HistoryValueInfo)
        self.assertEqual(history_info.total_count, 10)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockHistoryService())
    @patch.object(BaseAPI, 'parse_request')
    def test_stat_history(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        history_servicer = History()
        stat_info = history_servicer.stat({}, {})

        print_message(stat_info, 'test_stat_history')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
