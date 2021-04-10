import unittest
import copy
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
from spaceone.api.statistics.v1 import schedule_pb2
from spaceone.statistics.api.v1.schedule import Schedule
from test.factory.schedule_factory import ScheduleFactory


class _MockScheduleService(BaseService):

    def add(self, params):
        params = copy.deepcopy(params)
        if 'tags' in params:
            params['tags'] = utils.dict_to_tags(params['tags'])

        return ScheduleFactory(**params)

    def update(self, params):
        params = copy.deepcopy(params)
        if 'tags' in params:
            params['tags'] = utils.dict_to_tags(params['tags'])

        return ScheduleFactory(**params)

    def delete(self, params):
        pass

    def enable(self, params):
        return ScheduleFactory(**params)

    def disable(self, params):
        return ScheduleFactory(**params)

    def get(self, params):
        return ScheduleFactory(**params)

    def list(self, params):
        return ScheduleFactory.build_batch(10, **params), 10

    def stat(self, params):
        return {
            'results': [{'project_id': utils.generate_id('project'), 'server_count': 100}]
        }


class TestScheduleAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.statistics')
        connect('test', host='mongomock://localhost')
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_add_schedule(self, mock_parse_request, *args):
        params = {
            'topic': utils.random_string(),
            'options': {
                'aggregate': [
                    {
                        'query': {
                            'resource_type': 'identity.Project',
                            'query': {
                                'aggregate': [
                                    {
                                        'group': {
                                            'keys': [
                                                {
                                                    'key': 'project_id',
                                                    'name': 'project_id'
                                                },
                                                {
                                                    'key': 'name',
                                                    'name': 'project_name'
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        'join': {
                            'resource_type': 'inventory.Server',
                            'type': 'LEFT',
                            'keys': ['project_id'],
                            'query': {
                                'aggregate': [
                                    {
                                        'group': {
                                            'keys': [
                                                {
                                                    'key': 'project_id',
                                                    'name': 'project_id'
                                                }
                                            ],
                                            'fields': [
                                                {
                                                    'operator': 'count',
                                                    'name': 'server_count'
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        'join': {
                            'resource_type': 'inventory.CloudService',
                            'type': 'LEFT',
                            'keys': ['project_id'],
                            'query': {
                                'aggregate': [
                                    {
                                        'group': {
                                            'keys': [
                                                {
                                                    'key': 'project_id',
                                                    'name': 'project_id'
                                                }
                                            ],
                                            'fields': [
                                                {
                                                    'operator': 'count',
                                                    'name': 'cloud_service_count'
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        'formula': {
                            'eval': 'resource_count = server_count + cloud_service_count'
                        }
                    },
                    {
                        'sort': {
                            'key': 'resource_count',
                            'desc': True
                        }
                    }
                ],
                'page': {
                    'limit': 5
                }
            },
            'schedule': {
                'cron': '*/5 * * * *',
                'interval': 5,
                'minutes': [0, 10, 20, 30, 40, 50],
                'hours': [0, 6, 12, 18]
            },
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        schedule_servicer = Schedule()
        schedule_info = schedule_servicer.add({}, {})

        print_message(schedule_info, 'test_add_schedule')
        schedule_data = MessageToDict(schedule_info, preserving_proto_field_name=True)

        self.assertIsInstance(schedule_info, schedule_pb2.ScheduleInfo)
        self.assertEqual(schedule_info.topic, params['topic'])
        self.assertEqual(schedule_info.state, schedule_pb2.ScheduleInfo.State.ENABLED)
        self.assertEqual(schedule_data['options'], params['options'])
        self.assertDictEqual(schedule_data['schedule'], params['schedule'])
        self.assertDictEqual(schedule_data['tags'], params['tags'])
        self.assertEqual(schedule_info.domain_id, params['domain_id'])
        self.assertIsNotNone(getattr(schedule_info, 'created_at', None))

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_update_schedule(self, mock_parse_request, *args):
        params = {
            'schedule_id': utils.generate_id('schedule'),
            'schedule': {
                'cron': '* * * * *'
            },
            'tags': {
                'update_key': 'update_value'
            },
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        schedule_servicer = Schedule()
        schedule_info = schedule_servicer.update({}, {})

        print_message(schedule_info, 'test_update_schedule')
        schedule_data = MessageToDict(schedule_info, preserving_proto_field_name=True)

        self.assertIsInstance(schedule_info, schedule_pb2.ScheduleInfo)
        self.assertEqual(schedule_data['schedule'], params['schedule'])
        self.assertDictEqual(schedule_data['tags'], params['tags'])

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_delete_schedule(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        schedule_servicer = Schedule()
        result = schedule_servicer.delete({}, {})

        print_message(result, 'test_delete_schedule')

        self.assertIsInstance(result, Empty)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_enable_schedule(self, mock_parse_request, *args):
        params = {
            'schedule_id': utils.generate_id('schedule'),
            'state': 'ENABLED',
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        schedule_servicer = Schedule()
        schedule_info = schedule_servicer.enable({}, {})

        print_message(schedule_info, 'test_enable_schedule')

        self.assertIsInstance(schedule_info, schedule_pb2.ScheduleInfo)
        self.assertEqual(schedule_info.state, schedule_pb2.ScheduleInfo.State.ENABLED)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_disable_schedule(self, mock_parse_request, *args):
        params = {
            'schedule_id': utils.generate_id('schedule'),
            'state': 'DISABLED',
            'domain_id': utils.generate_id('domain')
        }
        mock_parse_request.return_value = (params, {})

        schedule_servicer = Schedule()
        schedule_info = schedule_servicer.disable({}, {})

        print_message(schedule_info, 'test_disable_schedule')

        self.assertIsInstance(schedule_info, schedule_pb2.ScheduleInfo)
        self.assertEqual(schedule_info.state, schedule_pb2.ScheduleInfo.State.DISABLED)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_get_schedule(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        schedule_servicer = Schedule()
        schedule_info = schedule_servicer.get({}, {})

        print_message(schedule_info, 'test_get_schedule')

        self.assertIsInstance(schedule_info, schedule_pb2.ScheduleInfo)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_list_schedules(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        schedule_servicer = Schedule()
        schedules_info = schedule_servicer.list({}, {})

        print_message(schedules_info, 'test_list_schedules')

        self.assertIsInstance(schedules_info, schedule_pb2.SchedulesInfo)
        self.assertIsInstance(schedules_info.results[0], schedule_pb2.ScheduleInfo)
        self.assertEqual(schedules_info.total_count, 10)

    @patch.object(BaseAPI, '__init__', return_value=None)
    @patch.object(Locator, 'get_service', return_value=_MockScheduleService())
    @patch.object(BaseAPI, 'parse_request')
    def test_stat_schedules(self, mock_parse_request, *args):
        mock_parse_request.return_value = ({}, {})

        schedule_servicer = Schedule()
        stat_info = schedule_servicer.stat({}, {})

        print_message(stat_info, 'test_stat_schedules')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
