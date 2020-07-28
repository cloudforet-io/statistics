import unittest
from unittest.mock import patch
from mongoengine import connect, disconnect

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.model.mongo_model import MongoModel
from spaceone.core.transaction import Transaction
from spaceone.statistics.error import *
from spaceone.statistics.service import ScheduleService
from spaceone.statistics.model.schedule_model import Schedule, JoinQuery, Formula, Scheduled, QueryOption
from spaceone.statistics.info.schedule_info import *
from spaceone.statistics.info.common_info import StatisticsInfo
from spaceone.statistics.connector.service_connector import ServiceConnector
from test.factory.schedule_factory import ScheduleFactory


class TestScheduleService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.statistics')
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'statistics',
            'api_class': 'Schedule'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(MongoModel, 'connect', return_value=None)
    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all schedules')
        schedule_vos = Schedule.objects.filter()
        schedule_vos.delete()

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, 'check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_add_schedule(self, mock_stat_resource, *args):
        mock_stat_resource.side_effect = [
            {
                'results': [{
                    'project_id': 'project-123',
                    'project_name': 'ncsoft',
                    'project_group_name': 'game'
                }, {
                    'project_id': 'project-456',
                    'project_name': 'nexon',
                    'project_group_name': 'game'
                }, {
                    'project_id': 'project-789',
                    'project_name': 'netmarble',
                    'project_group_name': 'game'
                }]
            }, {
                'results': [{
                    'project_id': 'project-123',
                    'server_count': 100
                }, {
                    'project_id': 'project-456',
                    'server_count': 65
                }, {
                    'project_id': 'project-789',
                    'server_count': 77
                }]
            }, {
                'results': [{
                    'project_id': 'project-123',
                    'cloud_service_count': 45
                }, {
                    'project_id': 'project-456',
                    'cloud_service_count': 87
                }, {
                    'project_id': 'project-789',
                    'cloud_service_count': 104
                }]
            }
        ]

        params = {
            'topic': 'project_server_and_cloud_service_count',
            'options': {
                'resource_type': 'identity.Project',
                'query': {
                    'aggregate': {
                        'group': {
                            'keys': [{
                                'key': 'project_id',
                                'name': 'project_id'
                            }, {
                                'key': 'name',
                                'name': 'project_name'
                            }, {
                                'key': 'project_group.name',
                                'name': 'project_group_name'
                            }],
                        }
                    },
                    'sort': {
                        'name': 'resource_count',
                        'desc': True
                    },
                    'page': {
                        'limit': 5
                    }
                },
                'join': [{
                    'keys': ['project_id'],
                    'resource_type': 'inventory.Server',
                    'query': {
                        'aggregate': {
                            'group': {
                                'keys': [{
                                    'key': 'project_id',
                                    'name': 'project_id'
                                }],
                                'fields': [{
                                    'operator': 'count',
                                    'name': 'server_count'
                                }]
                            }
                        }
                    }
                }, {
                    'keys': ['project_id'],
                    'resource_type': 'inventory.CloudService',
                    'query': {
                        'aggregate': {
                            'group': {
                                'keys': [{
                                    'key': 'project_id',
                                    'name': 'project_id'
                                }],
                                'fields': [{
                                    'operator': 'count',
                                    'name': 'cloud_service_count'
                                }]
                            }
                        }
                    }
                }],
                'formulas': [
                    {
                        'name': 'resource_count',
                        'formula': 'server_count + cloud_service_count'
                    }
                ]
            },
            'schedule': {
                'hours': [0, 6, 12, 18]
            },
            'tags': {
                'key': 'value'
            },
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'add'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vo = schedule_svc.add(params.copy())

        print_data(schedule_vo.to_dict(), 'test_add_schedule')
        ScheduleInfo(schedule_vo)

        self.assertIsInstance(schedule_vo, Schedule)
        self.assertEqual(params['topic'], schedule_vo.topic)
        self.assertEqual('ENABLED', schedule_vo.state)
        self.assertEqual(schedule_vo.options.resource_type, params['options']['resource_type'])
        self.assertDictEqual(schedule_vo.options.query, params['options']['query'])
        self.assertIsInstance(schedule_vo.options.join[0], JoinQuery)
        self.assertIsInstance(schedule_vo.options.formulas[0], Formula)
        self.assertIsInstance(schedule_vo.schedule, Scheduled)
        self.assertEqual(schedule_vo.schedule.hours, params['schedule']['hours'])
        self.assertEqual(params.get('tags', {}), schedule_vo.tags)
        self.assertEqual(params['domain_id'], schedule_vo.domain_id)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_update_schedule(self, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id)

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'schedule': {
                'cron': '*/5 * * * *'
            },
            'tags': {
                'update_key': 'update_value'
            },
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vo = schedule_svc.update(params.copy())

        print_data(schedule_vo.to_dict(), 'test_update_schedule')
        ScheduleInfo(schedule_vo)

        self.assertIsInstance(schedule_vo, Schedule)
        self.assertEqual(new_schedule_vo.schedule_id, schedule_vo.schedule_id)
        self.assertIsInstance(schedule_vo.schedule, Scheduled)
        self.assertEqual(schedule_vo.schedule.cron, params['schedule']['cron'])
        self.assertEqual(params.get('tags', {}), schedule_vo.tags)
        self.assertEqual(params['domain_id'], schedule_vo.domain_id)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_update_schedule_with_wrong_schedule_option(self, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id)

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'schedule': {
                'cron': '*/5 * * * *',
                'interval': 5
            },
            'tags': {
                'update_key': 'update_value'
            },
            'domain_id': self.domain_id
        }

        self.transaction.method = 'update'
        schedule_svc = ScheduleService(transaction=self.transaction)

        with self.assertRaises(ERROR_SCHEDULE_OPTION):
            schedule_vo = schedule_svc.update(params.copy())

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_enable_schedule(self, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id, state='DISABLED')

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'enable'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vo = schedule_svc.enable(params.copy())

        print_data(schedule_vo.to_dict(), 'test_enable_schedule')
        ScheduleInfo(schedule_vo)

        self.assertIsInstance(schedule_vo, Schedule)
        self.assertEqual(new_schedule_vo.schedule_id, schedule_vo.schedule_id)
        self.assertEqual('ENABLED', schedule_vo.state)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_disable_schedule(self, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id, state='ENABLED')

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'disable'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vo = schedule_svc.disable(params.copy())

        print_data(schedule_vo.to_dict(), 'test_disable_schedule')
        ScheduleInfo(schedule_vo)

        self.assertIsInstance(schedule_vo, Schedule)
        self.assertEqual(new_schedule_vo.schedule_id, schedule_vo.schedule_id)
        self.assertEqual('DISABLED', schedule_vo.state)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_delete_schedule(self, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id)

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'delete'
        schedule_svc = ScheduleService(transaction=self.transaction)
        result = schedule_svc.delete(params.copy())

        self.assertIsNone(result)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_get_schedule(self, *args):
        new_schedule_vo = ScheduleFactory(domain_id=self.domain_id)

        params = {
            'schedule_id': new_schedule_vo.schedule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'get'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vo = schedule_svc.get(params.copy())

        print_data(schedule_vo.to_dict(), 'test_get_schedule')
        ScheduleInfo(schedule_vo)

        self.assertIsInstance(schedule_vo, Schedule)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_list_schedules_by_schedule_id(self, *args):
        schedule_vos = ScheduleFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), schedule_vos))

        params = {
            'schedule_id': schedule_vos[0].schedule_id,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vos, total_count = schedule_svc.list(params.copy())
        SchedulesInfo(schedule_vos, total_count)

        self.assertEqual(len(schedule_vos), 1)
        self.assertIsInstance(schedule_vos[0], Schedule)
        self.assertEqual(total_count, 1)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_list_schedules_by_topic(self, *args):
        schedule_vos = ScheduleFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), schedule_vos))

        params = {
            'topic': schedule_vos[0].topic,
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vos, total_count = schedule_svc.list(params.copy())
        SchedulesInfo(schedule_vos, total_count)

        self.assertEqual(len(schedule_vos), 1)
        self.assertIsInstance(schedule_vos[0], Schedule)
        self.assertEqual(total_count, 1)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_list_schedules_by_tag(self, *args):
        ScheduleFactory(tags={'tag_key': 'tag_value'}, domain_id=self.domain_id)
        schedule_vos = ScheduleFactory.build_batch(9, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), schedule_vos))

        params = {
            'query': {
                'filter': [{
                    'k': 'tags.tag_key',
                    'v': 'tag_value',
                    'o': 'eq'
                }]
            },
            'domain_id': self.domain_id
        }

        self.transaction.method = 'list'
        schedule_svc = ScheduleService(transaction=self.transaction)
        schedule_vos, total_count = schedule_svc.list(params.copy())
        SchedulesInfo(schedule_vos, total_count)

        self.assertEqual(len(schedule_vos), 1)
        self.assertIsInstance(schedule_vos[0], Schedule)
        self.assertEqual(total_count, 1)

    @patch.object(MongoModel, 'connect', return_value=None)
    def test_stat_schedule(self, *args):
        schedule_vos = ScheduleFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), schedule_vos))

        params = {
            'domain_id': self.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'schedule_id',
                            'name': 'Id'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Count'
                        }]
                    }
                },
                'sort': {
                    'name': 'Count',
                    'desc': True
                }
            }
        }

        self.transaction.method = 'stat'
        schedule_svc = ScheduleService(transaction=self.transaction)
        values = schedule_svc.stat(params)
        StatisticsInfo(values)

        print_data(values, 'test_stat_schedule')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
