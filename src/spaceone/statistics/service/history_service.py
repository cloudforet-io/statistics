import logging
from datetime import datetime

from spaceone.core import utils
from spaceone.core.service import *
from spaceone.statistics.error import *
from spaceone.statistics.manager.resource_manager import ResourceManager
from spaceone.statistics.manager.schedule_manager import ScheduleManager
from spaceone.statistics.manager.history_manager import HistoryManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class HistoryService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager('ResourceManager')
        self.history_mgr: HistoryManager = self.locator.get_manager('HistoryManager')

    @transaction
    @check_required(['schedule_id', 'domain_id'])
    def create(self, params):
        """Statistics query to resource

        Args:
            params (dict): {
                'schedule_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        schedule_mgr: ScheduleManager = self.locator.get_manager('ScheduleManager')

        domain_id = params['domain_id']
        schedule_id = params['schedule_id']

        schedule_vo = schedule_mgr.get_schedule(schedule_id, domain_id, ['topic', 'options'])
        schedule_data = schedule_vo.to_dict()
        topic = schedule_data['topic']
        options = schedule_data['options']
        resource_type = options['resource_type']
        query = options.get('query', {})
        join = list(map(lambda j: j.to_dict(), options.get('join', [])))
        formulas = list(map(lambda f: f.to_dict(), options.get('formulas', [])))
        sort = query.get('sort')
        page = query.get('page', {})
        limit = query.get('limit')

        if len(join) > 0 or len(formulas) > 0 or limit:
            query['sort'] = None
            query['page'] = None
            query['limit'] = None

        results = self.resource_mgr.stat(resource_type, query, domain_id)
        if len(join) > 0 or len(formulas) > 0:
            results = self.resource_mgr.join_and_execute_formula(results, resource_type,
                                                                 query, join, formulas, sort,
                                                                 page, limit, domain_id)

        self.history_mgr.create_history(schedule_vo, topic, results, domain_id)

    @transaction
    @check_required(['topic', 'domain_id'])
    @append_query_filter(['topic', 'domain_id'])
    @append_keyword_filter(['topic'])
    def list(self, params):
        """ List history

        Args:
            params (dict): {
                'topic': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            history_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.history_mgr.list_history(query)

    @transaction
    @check_required(['topic', 'query', 'domain_id'])
    @append_query_filter(['topic', 'domain_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.history_mgr.stat_history(query)

    @transaction
    @check_required(['topic', 'from', 'default_fields', 'diff_fields', 'domain_id'])
    def diff(self, params):
        """
        Args:
            params (dict): {
                'filter': 'list',
                'filter_or': 'list',
                'from': 'timediff',
                'to': 'timediff',
                'default_fields': 'list',
                'diff_fields': 'list',
                'domain_id': 'str'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        domain_id = params['domain_id']
        topic = params['topic']
        diff_filter = params.get('filter', [])
        diff_filter_or = params.get('filter_or', [])
        diff_from = self._change_timediff_value('from', params['from'])
        diff_to = self._change_timediff_value('to', params.get('to', 'now'))
        default_fields = params['default_fields']
        diff_fields = params['diff_fields']

        self._check_from_to_date(diff_from, diff_to)
        from_data = self._get_history_data(topic, diff_filter[:], diff_filter_or[:], diff_from, domain_id)
        to_data = self._get_history_data(topic, diff_filter[:], diff_filter_or[:], diff_to, domain_id, diff_from)

        return self.history_mgr.diff_history(from_data, to_data, default_fields, diff_fields)

    @staticmethod
    def _check_from_to_date(diff_from, diff_to):
        if diff_from > diff_to:
            raise ERROR_DIFF_TIME_RANGE()

    @staticmethod
    def _change_timediff_value(key, value):
        try:
            return utils.parse_timediff_query(value)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER(key=key, reason=e)

    def _get_history_data(self, topic, diff_filter, diff_filter_or, diff_datetime, domain_id, max_datetime=None):
        query = self._make_statistics_query(topic, diff_filter, diff_filter_or, diff_datetime, domain_id, max_datetime)
        results = self.history_mgr.stat_history(query)
        if len(results) > 0:
            return results[0]['data']
        else:
            return {}

    @staticmethod
    def _make_statistics_query(topic, diff_filter, diff_filter_or, diff_datetime, domain_id, max_datetime):
        diff_filter += [{
            'k': 'topic',
            'v': topic,
            'o': 'eq'
        }, {
            'k': 'domain_id',
            'v': domain_id,
            'o': 'eq'
        }, {
            'k': 'created_at',
            'v': diff_datetime,
            'o': 'lte'
        }]

        if max_datetime:
            diff_filter.append({
                'k': 'created_at',
                'v': max_datetime,
                'o': 'gte'
            })

        _query = {
            'filter': diff_filter,
            'filter_or': diff_filter_or,
            'aggregate': {
                'group': {
                    'keys': [{
                        'k': 'created_at',
                        'n': 'created_at'
                    }],
                    'fields': [{
                        'k': 'values',
                        'o': 'add_to_set',
                        'n': 'data'
                    }]
                }
            },
            'sort': {
                'name': 'created_at'
            },
            'page': {
                'limit': 1
            }
        }

        return _query
