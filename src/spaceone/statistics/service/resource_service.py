import logging

from spaceone.core.service import *

from spaceone.statistics.manager.resource_manager import ResourceManager

_LOGGER = logging.getLogger(__name__)


class ResourceService(BaseService):
    resource = "Resource"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager('ResourceManager')

    @transaction()
    @check_required(['aggregate'])
    def stat(self, params):
        """Statistics query to resource

        Args:
            params (dict): {
                'aggregate': 'list',      # required
                'page': 'dict',
            }

        Returns:
            stat_info (object)
        """
        aggregate = params.get('aggregate', [])
        page = params.get('page', {})

        return self.resource_mgr.stat(aggregate, page)
