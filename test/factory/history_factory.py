import factory
import random

from spaceone.core import utils
from spaceone.statistics.model.history_model import History


class ValueFactory(factory.DictFactory):

    project_id = factory.LazyAttribute(lambda o: utils.generate_id('project'))
    server_count = factory.LazyAttribute(lambda o: random.randrange(0, 1000))


class HistoryFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = History

    topic = factory.LazyAttribute(lambda o: utils.random_string())
    values = factory.SubFactory(ValueFactory)
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')
