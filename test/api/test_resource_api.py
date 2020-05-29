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
from spaceone.statistics.api.v1.resource import Resource
from test.factory.resource_factory import StatFactory


class _MockStatService(BaseService):

    def stat(self, params):
        return StatFactory(**params)['results']


class TestResourceAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(service='statistics')
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockStatService())
    @patch.object(BaseAPI, 'parse_request')
    def test_resource_stat(self, mock_parse_request, *args):
        params = {}
        mock_parse_request.return_value = ({}, {})

        resource_servicer = Resource()
        stat_info = resource_servicer.stat({}, {})

        print_message(stat_info, 'test_resource_stat')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
