import functools
from spaceone.api.statistics.v1 import history_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.statistics.model.history_model import History

__all__ = ['HistoryInfo']


def HistoryValueInfo(history_vo: History, minimal=False):
    info = {
        'topic': history_vo.topic,
        'values': change_struct_type(history_vo.values) if history_vo.values else None,
        'created_at': change_timestamp_type(history_vo.created_at),
        'domain_id': history_vo.domain_id
    }

    return history_pb2.HistoryValueInfo(**info)


def HistoryInfo(schedule_vos, total_count, **kwargs):
    return history_pb2.HistoryInfo(results=list(
        map(functools.partial(HistoryValueInfo, **kwargs), schedule_vos)), total_count=total_count)
