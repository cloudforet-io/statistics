from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.statistics.model.schedule_model import Schedule


class History(MongoModel):
    topic = StringField(max_length=255)
    schedule = ReferenceField('Schedule', reverse_delete_rule=CASCADE)
    values = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(required=True)

    meta = {
        'updatable_fields': [],
        'exact_fields': [
            'domain_id'
        ],
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            'schedule',
            'domain_id',
            'created_at'
        ]
    }
