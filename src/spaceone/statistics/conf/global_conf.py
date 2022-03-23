DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    'default': {
        'db': 'statistics',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}

CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 300
    }
}

HANDLERS = {
}

CONNECTORS = {
    'ServiceConnector': {
        'statistics': 'grpc://localhost:50051/v1',
        'identity': 'grpc://identity:50051/v1',
        'inventory': 'grpc://inventory:50051/v1',
        'monitoring': 'grpc://monitoring:50051/v1',
        'cost_analysis': 'grpc://cost-analysis:50051/v1',
        'notification': 'grpc://notification:50051/v1',
        'config': 'grpc://config:50051/v1',
        'secret': 'grpc://secret:50051/v1',
    }
}

ENDPOINTS = {}
LOG = {}
QUEUES = {}
SCHEDULERS = {}
WORKERS = {}
TOKEN = ""
TOKEN_INFO = {}
CELERY = {}