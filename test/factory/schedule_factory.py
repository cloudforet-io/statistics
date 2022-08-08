import factory

from spaceone.core import utils
from spaceone.statistics.model.schedule_model import Schedule, Scheduled


class ScheduledFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = Scheduled

    cron = '*/5 * * * *'
    interval = 5
    minutes = [0, 10, 20, 30, 40, 50]
    hours = [0, 6, 12, 18]


class ScheduleFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = Schedule

    schedule_id = factory.LazyAttribute(lambda o: utils.generate_id('schedule'))
    topic = factory.LazyAttribute(lambda o: utils.random_string())
    state = 'ENABLED'
    options = {
        'aggregate': [
            {
                'query': {
                    'resource_type': 'inventory.Server',
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
                                            'name': 'count'
                                        }
                                    ]
                                }
                            },
                            {
                                'sort': {
                                    'key': 'count'
                                }
                            }
                        ]
                    }
                }
            }
        ],
        'page': {
            'limit': 5
        }
    }
    schedule = factory.SubFactory(ScheduledFactory)
    tags = {'tag_key': 'tag_value'}
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')
    last_scheduled_at = None
