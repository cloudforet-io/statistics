import logging

from spaceone.core.service import *

from spaceone.statistics.error import *
from spaceone.statistics.manager.resource_manager import ResourceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ResourceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager('ResourceManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['aggregate', 'domain_id'])
    def stat(self, params):
        """Statistics query to resource

        Args:
            params (dict): {
                'aggregate': 'list',
                'page': 'dict',
                'domain_id': 'str'
            }

        Returns:
            stat_info (object)
        """
        aggregate = params.get('aggregate', [])
        page = params.get('page', {})
        domain_id = params['domain_id']

        return self.resource_mgr.stat(aggregate, page, domain_id)
