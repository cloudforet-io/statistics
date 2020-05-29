import unittest
import os
from unittest.mock import patch

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.statistics.connector.service_connector import ServiceConnector


class TestServiceConnector(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(service='statistics')
        config_path = os.environ.get('TEST_CONFIG')
        test_config = utils.load_yaml_from_file(config_path)

        cls.transaction = Transaction({
            'token': test_config.get('access_token')
        })

        cls.domain_id = test_config.get('domain_id')
        cls.connector_conf = test_config.get('ServiceConnector')
        cls.service_connector = ServiceConnector(cls.transaction, cls.connector_conf)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()

    def test_stat_identity_project(self):
        query = {
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
                    }]
                }
            },
            'page': {
                'start': 3,
                'limit': 3
            }
        }
        self.service_connector.check_resource_type('identity', 'Project')
        response = self.service_connector.stat_resource('identity', 'Project', query, self.domain_id)
        print_data(response, 'test_stat_identity_project')

    def test_stat_inventory_server(self):
        query = {
            'filter': [{
                'k': 'project_id',
                'v': None,
                'o': 'not'
            }],
            'aggregate': {
                'group': {
                    'keys': [{
                        'key': 'project_id',
                        'name': 'project_id'
                    }],
                    'fields': [{
                        'operator': 'count',
                        'name': 'server_count'
                    }, {
                        'key': 'data.hardware.core',
                        'operator': 'sum',
                        'name': 'total_core'
                    }]
                }
            },
            'sort': {
                'name': 'server_count',
                'desc': True
            },
            'page': {
                'limit': 2
            }
        }
        self.service_connector.check_resource_type('inventory', 'Server')
        response = self.service_connector.stat_resource('inventory', 'Server', query, self.domain_id)
        print_data(response, 'test_stat_inventory_server')

    def test_stat_inventory_server_per_project(self):
        query = {
            'filter': [{
                'k': 'project_id',
                'v': None,
                'o': 'not'
            }],
            'aggregate': {
                'group': {
                    'keys': [{
                        'key': 'project_id',
                        'name': 'project_id'
                    }],
                    'fields': [{
                        'operator': 'count',
                        'name': 'server_count'
                    }, {
                        'key': 'data.hardware.core',
                        'operator': 'sum',
                        'name': 'total_core'
                    }]
                }
            },
            'sort': {
                'name': 'server_count',
                'desc': True
            }
        }
        self.service_connector.check_resource_type('inventory', 'Server')
        response = self.service_connector.stat_resource('inventory', 'Server', query, self.domain_id)
        print_data(response, 'test_stat_inventory_server_per_project')

    def test_stat_inventory_server_per_region(self):
        query = {
            'filter': [{
                'k': 'data.compute.region_name',
                'v': None,
                'o': 'not'
            }],
            'aggregate': {
                'group': {
                    'keys': [{
                        'key': 'data.compute.region_name',
                        'name': 'region_name'
                    }, {
                        'key': 'provider',
                        'name': 'provider'
                    }],
                    'fields': [{
                        'operator': 'count',
                        'name': 'server_count'
                    }]
                }
            },
            'sort': {
                'name': 'server_count',
                'desc': True
            }
        }
        self.service_connector.check_resource_type('inventory', 'Server')
        response = self.service_connector.stat_resource('inventory', 'Server', query, self.domain_id)
        print_data(response, 'test_stat_inventory_server_per_region')

    def test_stat_inventory_server_per_instance_type(self):
        query = {
            'aggregate': {
                'group': {
                    'keys': [{
                        'key': 'data.compute.instance_type',
                        'name': 'instance_type'
                    }],
                    'fields': [{
                        'operator': 'count',
                        'name': 'server_count'
                    }, {
                        'key': 'data.hardware.core',
                        'operator': 'sum',
                        'name': 'total_core'
                    }]
                }
            },
            'sort': {
                'name': 'server_count',
                'desc': True
            }
        }
        self.service_connector.check_resource_type('inventory', 'Server')
        response = self.service_connector.stat_resource('inventory', 'Server', query, self.domain_id)
        print_data(response, 'test_stat_inventory_server_per_instance_type')

    def test_stat_inventory_cloud_service_per_region(self):
        query = {
            # 'filter': [{
            #     'k': 'data.region_name',
            #     'v': None,
            #     'o': 'not'
            # }],
            'aggregate': {
                'group': {
                    'keys': [{
                        'key': 'data.region_name',
                        'name': 'region_name'
                    }],
                    'fields': [{
                        'operator': 'count',
                        'name': 'cloud_service_count'
                    }]
                }
            },
            'sort': {
                'name': 'cloud_service_count',
                'desc': True
            }
        }
        self.service_connector.check_resource_type('inventory', 'CloudService')
        response = self.service_connector.stat_resource('inventory', 'CloudService', query, self.domain_id)
        print_data(response, 'test_stat_inventory_cloud_service_per_region')

    def test_stat_inventory_cloud_service_per_project(self):
        query = {
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
            },
            'sort': {
                'name': 'cloud_service_count',
                'desc': True
            }
        }
        self.service_connector.check_resource_type('inventory', 'CloudService')
        response = self.service_connector.stat_resource('inventory', 'CloudService', query, self.domain_id)
        print_data(response, 'test_stat_inventory_cloud_service_per_project')

    def test_base_query(self):
        query = {
            "aggregate": {
                    "group": {
                        "keys": [
                            {
                                "key": "project_id",
                                "name": "project_id"
                            }
                        ],
                        "fields": [
                            {
                                "operator": "count",
                                "name": "server_count"
                            }
                        ]
                    }
                }
        }
        self.service_connector.check_resource_type('identity', 'Project')
        response = self.service_connector.stat_resource('identity', 'Project', query, self.domain_id)
        print_data(response, 'test_base_query')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
