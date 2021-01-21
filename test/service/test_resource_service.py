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
from spaceone.statistics.service.resource_service import ResourceService
from spaceone.statistics.info.common_info import StatisticsInfo
from spaceone.statistics.connector.service_connector import ServiceConnector
from test.factory.resource_factory import StatFactory


class TestResourceService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.statistics')
        config.set_service_config()
        config.set_global(DATABASE_SUPPORT_AWS_DOCUMENT_DB=True)
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'statistics',
            'api_class': 'Resource'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(MongoModel, 'connect', return_value=None)
    def tearDown(self, *args) -> None:
        print()

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, '_check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_resource_stat_join_and_formula(self, mock_stat_resource, *args):
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
                    'project_name': 'ncsoft',
                    'server_count': 100
                }, {
                    'project_id': 'project-456',
                    'project_name': 'nexon',
                    'server_count': 65
                }, {
                    'project_id': 'project-789',
                    'project_name': 'netmarble',
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
                }
            },
            'join': [{
                'keys': ['project_id', 'project_name'],
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
                    'formula': 'resource_count = server_count + cloud_service_count'
                }
            ],
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'stat'
        resource_svc = ResourceService(transaction=self.transaction)
        results = resource_svc.stat(params.copy())

        print_data(results, 'test_resource_stat_join_and_formula')
        StatisticsInfo(results)

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, '_check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_resource_stat_index_join(self, mock_stat_resource, *args):
        mock_stat_resource.side_effect = [
            {
                'results': [{
                    'success_count': 17
                }]
            }, {
                'results': [{
                    'fail_count': 0
                }]
            }
        ]

        params = {
            'resource_type': 'inventory.Job',
            'query': {
                'filter': [{
                    'key': 'state',
                    'value': 'SUCCESS',
                    'operator': 'eq'
                }],
                'aggregate': {
                    'count': {
                        'name': 'success_count'
                    }
                }
            },
            'join': [{
                'resource_type': 'inventory.Jon',
                'query': {
                    'filter': [{
                        'key': 'state',
                        'value': 'FAILURE',
                        'operator': 'eq'
                    }],
                    'aggregate': {
                        'count': {
                            'name': 'fail_count'
                        }
                    }
                }
            }],
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'stat'
        resource_svc = ResourceService(transaction=self.transaction)
        results = resource_svc.stat(params.copy())

        print_data(results, 'test_resource_stat_index_join')
        StatisticsInfo(results)

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, '_check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_resource_stat_index_join_wrong_index(self, mock_stat_resource, *args):
        mock_stat_resource.side_effect = [
            {
                'results': []
            }, {
                'results': [{
                    'fail_count': 0
                }]
            }
        ]

        params = {
            'resource_type': 'inventory.Job',
            'query': {
                'filter': [{
                    'key': 'state',
                    'value': 'SUCCESS',
                    'operator': 'eq'
                }],
                'aggregate': {
                    'count': {
                        'name': 'success_count'
                    }
                }
            },
            'join': [{
                'type': 'OUTER',
                'resource_type': 'inventory.Jon',
                'query': {
                    'filter': [{
                        'key': 'state',
                        'value': 'FAILURE',
                        'operator': 'eq'
                    }],
                    'aggregate': {
                        'count': {
                            'name': 'fail_count'
                        }
                    }
                }
            }],
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'stat'
        resource_svc = ResourceService(transaction=self.transaction)
        results = resource_svc.stat(params.copy())

        print_data(results, 'test_resource_stat_index_join_wrong_index')
        StatisticsInfo(results)

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, '_check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_resource_stat_empty_join(self, mock_stat_resource, *args):
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
                'results': []
            }
        ]

        params = {
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
                'page': {
                    'start': 2,
                    'limit': 1
                }
            },
            'join': [{
                'keys': ['project_id', 'project_name'],
                'resource_type': 'inventory.Server',
                'query': {
                    'aggregate': {
                        'group': {
                            'keys': [{
                                'key': 'project_id',
                                'name': 'project_id'
                            }, {
                                'key': 'name',
                                'name': 'project_name'
                            }],
                            'fields': [{
                                'operator': 'count',
                                'name': 'server_count'
                            }]
                        }
                    }
                }
            }],
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'stat'
        resource_svc = ResourceService(transaction=self.transaction)
        results = resource_svc.stat(params.copy())

        print_data(results, 'test_resource_stat_empty_join')
        StatisticsInfo(results)

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, '_check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_resource_stat_distinct(self, mock_stat_resource, *args):
        mock_stat_resource.side_effect = [
            {
                'results': [
                    utils.generate_id('project'),
                    utils.generate_id('project'),
                    utils.generate_id('project'),
                    utils.generate_id('project'),
                    utils.generate_id('project')
                ],
                'total_count': 24
            }
        ]

        params = {
            'resource_type': 'identity.Project',
            'query': {
                'distinct': 'project_id',
                'page': {
                    'start': 5,
                    'limit': 5
                }
            },
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'stat'
        resource_svc = ResourceService(transaction=self.transaction)
        results = resource_svc.stat(params.copy())

        print_data(results, 'test_resource_stat_distinct')
        StatisticsInfo(results)

    @patch.object(MongoModel, 'connect', return_value=None)
    @patch.object(ServiceConnector, '_check_resource_type', return_value=None)
    @patch.object(ServiceConnector, 'stat_resource')
    def test_resource_stat_distinct_with_join(self, mock_stat_resource, *args):
        mock_stat_resource.side_effect = [
            {
                'results': [
                    utils.generate_id('project'),
                    utils.generate_id('project'),
                    utils.generate_id('project'),
                    utils.generate_id('project'),
                    utils.generate_id('project')
                ],
                'total_count': 24
            }
        ]

        params = {
            'resource_type': 'identity.Project',
            'query': {
                'distinct': 'project_id',
                'page': {
                    'start': 5,
                    'limit': 5
                }
            },
            'join': [{
                'keys': ['project_id', 'project_name'],
                'resource_type': 'inventory.Server',
                'query': {
                    'aggregate': {
                        'group': {
                            'keys': [{
                                'key': 'project_id',
                                'name': 'project_id'
                            }, {
                                'key': 'name',
                                'name': 'project_name'
                            }],
                            'fields': [{
                                'operator': 'count',
                                'name': 'server_count'
                            }]
                        }
                    }
                }
            }],
            'domain_id': utils.generate_id('domain')
        }

        self.transaction.method = 'stat'
        resource_svc = ResourceService(transaction=self.transaction)
        with self.assertRaises(ERROR_STATISTICS_DISTINCT):
            resource_svc.stat(params.copy())


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
