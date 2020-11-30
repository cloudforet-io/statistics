import factory

from spaceone.core import utils
from spaceone.statistics.model.schedule_model import Schedule, Scheduled, JoinQuery, Formula, QueryOption


class ScheduledFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = Scheduled

    cron = '*/5 * * * *'
    interval = 5
    minutes = [0, 10, 20, 30, 40, 50]
    hours = [0, 6, 12, 18]


class JoinQueryFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = JoinQuery

    keys = ['project_id']
    type = 'LEFT'
    data_source_id = factory.LazyAttribute(lambda o: utils.generate_id('ds'))
    resource_type = 'inventory.Server'
    query = {
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


class FormulaFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = Formula

    formula = 'z = a + (b / c)'


class QueryOptionFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = QueryOption

    data_source_id = factory.LazyAttribute(lambda o: utils.generate_id('ds'))
    resource_type = 'identity.Project'
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
    }
    join = factory.List([factory.SubFactory(JoinQueryFactory)])
    formulas = factory.List([factory.SubFactory(FormulaFactory)])


class ScheduleFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = Schedule

    schedule_id = factory.LazyAttribute(lambda o: utils.generate_id('schedule'))
    topic = factory.LazyAttribute(lambda o: utils.random_string())
    state = 'ENABLED'
    options = factory.SubFactory(QueryOptionFactory)
    schedule = factory.SubFactory(ScheduledFactory)
    tags = [
        {
            'key': 'tag_key',
            'value': 'tag_value'
        }
    ]
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')
    last_scheduled_at = None
