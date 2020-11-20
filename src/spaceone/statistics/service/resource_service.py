import logging

from spaceone.core.service import *

from spaceone.statistics.error import *
from spaceone.statistics.manager.resource_manager import ResourceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class ResourceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager('ResourceManager')

    @transaction
    @check_required(['resource_type', 'query', 'domain_id'])
    def stat(self, params):
        """Statistics query to resource

        Args:
            params (dict): {
                'data_source_id': 'str',
                'resource_type': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'join': 'list',
                'concat': 'list',
                'extend_data': 'dict',
                'fill_na': 'dict',
                'formulas': 'list',
                'domain_id': 'str'
            }

        Returns:
            stat_info (object)
        """
        domain_id = params['domain_id']
        resource_type = params['resource_type']
        query = params.get('query', {})
        distinct = query.get('distinct')
        extend_data = params.get('extend_data', {})
        fill_na = params.get('fill_na', {})
        join = params.get('join', [])
        concat = params.get('concat', [])
        formulas = params.get('formulas', [])
        sort = query.get('sort')
        page = query.get('page', {})
        limit = query.get('limit')
        has_additional_stat = len(extend_data.keys()) > 0 or len(join) > 0 or len(concat) > 0 or len(formulas) > 0

        if distinct:
            if has_additional_stat:
                raise ERROR_STATISTICS_DISTINCT()
        else:
            if has_additional_stat:
                query['sort'] = None
                query['page'] = None
                query['limit'] = None

        response = self.resource_mgr.stat(resource_type, query, domain_id)
        if has_additional_stat:
            results = response.get('results', [])
            response = self.resource_mgr.execute_additional_stat(results, resource_type, query, extend_data,
                                                                 join, concat, fill_na, formulas,
                                                                 sort, page, limit, domain_id)
        return response
