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
                'formulas': 'list',
                'domain_id': 'str'
            }

        Returns:
            stat_info (object)
        """
        domain_id = params['domain_id']
        resource_type = params['resource_type']
        query = params.get('query', {})
        join = params.get('join', [])
        formulas = params.get('formulas', [])
        sort = query.get('sort')
        page = query.get('page', {})
        limit = query.get('limit')

        if len(join) > 0 or len(formulas) > 0 or limit:
            query['sort'] = None
            query['page'] = None
            query['limit'] = None

        results = self.resource_mgr.stat(resource_type, query, domain_id)
        if len(join) > 0 or len(formulas) > 0:
            results = self.resource_mgr.join_and_execute_formula(results, resource_type, query, join,
                                                                 formulas, sort, page, limit, domain_id)
        return results
