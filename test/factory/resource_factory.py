import factory
import time
import random

from spaceone.core import utils


class ValueFactory(factory.DictFactory):

    project_id = factory.LazyAttribute(lambda o: utils.generate_id('project'))
    server_count = factory.LazyAttribute(lambda o: random.randrange(0, 1000))


class StatFactory(factory.DictFactory):

    results = factory.List([
        factory.SubFactory(ValueFactory) for _ in range(5)
    ])
