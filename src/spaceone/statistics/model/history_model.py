from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.statistics.model.schedule_model import Schedule


class History(MongoModel):
    topic = StringField(max_length=255)
    schedule = ReferenceField("Schedule", reverse_delete_rule=NULLIFY)
    values = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(required=True)

    meta = {
        "updatable_fields": [],
        "change_query_keys": {
            "user_projects": "values.project_id",
            "project_id": "values.project_id",
            "workspace_id": "values.workspace_id",
        },
        "ordering": ["-created_at"],
        "indexes": [
            "topic",
            "schedule",
            "created_at",
            "values.project_id",
            "values.workspace_id",
            "domain_id",
        ],
    }
