from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Scheduled(EmbeddedDocument):
    cron = StringField(max_length=1024, default=None, null=True)
    interval = IntField(min_value=1, max_value=60, default=None, null=True)
    minutes = ListField(IntField(), default=None, null=True)
    hours = ListField(IntField(), default=None, null=True)

    def to_dict(self):
        return self.to_mongo()


class JoinQuery(EmbeddedDocument):
    keys = ListField(StringField(max_length=40))
    type = StringField(max_length=20, default='LEFT', choices=('LEFT', 'RIGHT', 'OUTER', 'INNER'))
    resource_type = StringField(max_length=80)
    data_source_id = StringField(max_length=40, default=None, null=True)
    resource_type = StringField(max_length=80)
    query = DictField()

    def to_dict(self):
        return self.to_mongo()


class Formula(EmbeddedDocument):
    name = StringField(max_length=40)
    formula = StringField()

    def to_dict(self):
        return self.to_mongo()


class QueryOption(EmbeddedDocument):
    data_source_id = StringField(max_length=40, default=None, null=True)
    resource_type = StringField(max_length=80)
    query = DictField()
    join = ListField(EmbeddedDocumentField(JoinQuery))
    formulas = ListField(EmbeddedDocumentField(Formula))

    def to_dict(self):
        return self.to_mongo()


class Schedule(MongoModel):
    schedule_id = StringField(max_length=40, generate_id='sch', unique=True)
    topic = StringField(max_length=255, unique_with='domain_id')
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    options = EmbeddedDocumentField(QueryOption, required=True)
    schedule = EmbeddedDocumentField(Scheduled, default=Scheduled)
    tags = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    last_scheduled_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'schedule',
            'state',
            'tags',
            'last_scheduled_at'
        ],
        'exact_fields': [
            'schedule_id',
            'state',
            'options.data_source_id',
            'domain_id'
        ],
        'minimal_fields': [
            'schedule_id',
            'topic',
            'state'
        ],
        'ordering': [
            'topic'
        ],
        'indexes': [
            'schedule_id',
            'topic',
            'state',
            'options.data_source_id',
            'options.resource_type',
            'domain_id'
        ]
    }
