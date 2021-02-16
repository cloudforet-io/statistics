from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Scheduled(EmbeddedDocument):
    cron = StringField(max_length=1024, default=None, null=True)
    interval = IntField(min_value=1, max_value=60, default=None, null=True)
    minutes = ListField(IntField(), default=None, null=True)
    hours = ListField(IntField(), default=None, null=True)

    def to_dict(self):
        return self.to_mongo()


class ScheduleTag(EmbeddedDocument):
    key = StringField(max_length=255)
    value = StringField(max_length=255)


class Schedule(MongoModel):
    schedule_id = StringField(max_length=40, generate_id='sch', unique=True)
    topic = StringField(max_length=255, unique_with='domain_id')
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    options = DictField(required=True)
    schedule = EmbeddedDocumentField(Scheduled, default=Scheduled)
    tags = ListField(EmbeddedDocumentField(ScheduleTag))
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
            'domain_id',
            ('tags.key', 'tags.value')
        ]
    }
