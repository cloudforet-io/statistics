import functools
from spaceone.api.core.v1 import tag_pb2
from spaceone.api.statistics.v1 import schedule_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.statistics.model.schedule_model import Schedule, Scheduled

__all__ = ['ScheduleInfo', 'SchedulesInfo']


def ScheduledInfo(vo: Scheduled):
    info = {
        'cron': vo.cron,
        'interval': vo.interval,
        'hours': vo.hours,
        'minutes': vo.minutes
    }
    return schedule_pb2.Scheduled(**info)


def ScheduleInfo(schedule_vo: Schedule, minimal=False):
    info = {
        'schedule_id': schedule_vo.schedule_id,
        'topic': schedule_vo.topic,
        'state': schedule_vo.state,
    }

    if not minimal:
        info.update({
            'options': change_struct_type(schedule_vo.options.to_dict()) if schedule_vo.options else None,
            'schedule': ScheduledInfo(schedule_vo.schedule) if schedule_vo.schedule else None,
            'tags': [tag_pb2.Tag(key=tag.key, value=tag.value) for tag in schedule_vo.tags],
            'domain_id': schedule_vo.domain_id,
            'created_at': change_timestamp_type(schedule_vo.created_at),
            'last_scheduled_at': change_timestamp_type(schedule_vo.last_scheduled_at)
        })

    return schedule_pb2.ScheduleInfo(**info)


def SchedulesInfo(schedule_vos, total_count, **kwargs):
    return schedule_pb2.SchedulesInfo(results=list(
        map(functools.partial(ScheduleInfo, **kwargs), schedule_vos)), total_count=total_count)
