import logging

from spaceone.core.service import *
from spaceone.statistics.error import *
from spaceone.statistics.manager.resource_manager import ResourceManager
from spaceone.statistics.manager.schedule_manager import ScheduleManager
from spaceone.statistics.manager.history_manager import HistoryManager
from spaceone.statistics.model import History

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class HistoryService(BaseService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager("ResourceManager")
        self.history_mgr: HistoryManager = self.locator.get_manager("HistoryManager")

    @transaction(permission="statistics:History.write", role_types=["DOMAIN_ADMIN"])
    @check_required(["schedule_id", "domain_id"])
    def create(self, params: dict) -> None:
        """Statistics query to resource

        Args:
            params (dict): {
                'schedule_id': 'str',  # required
                'domain_id': 'str'     # injected from auth (required)
            }

        Returns:
            None
        """

        schedule_mgr: ScheduleManager = self.locator.get_manager("ScheduleManager")

        domain_id = params["domain_id"]
        schedule_id = params["schedule_id"]

        schedule_vo = schedule_mgr.get_schedule(schedule_id, domain_id)
        topic = schedule_vo.topic
        options = schedule_vo.options
        aggregate = options.get("aggregate", [])
        page = params.get("page", {})

        response = self.resource_mgr.stat(aggregate, page, domain_id)

        results = response.get("results", [])
        self.history_mgr.create_history(schedule_vo, topic, results, domain_id)

    @transaction(
        permission="statistics:History.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["domain_id"])
    @append_query_filter(["topic", "domain_id", "workspace_id", "user_projects"])
    @append_keyword_filter(["topic"])
    def list(self, params):
        """List history

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'topic': 'str',
                'domain_id': 'str',                            # injected from auth
                'workspace_id': 'str',                         # injected from auth
                'user_projects': 'list'                        # injected from auth
            }

        Returns:
            history_vos (object)
            total_count
        """

        query = params.get("query", {})
        return self.history_mgr.list_history(query)

    @transaction(
        permission="statistics:History.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["topic", "domain_id", "workspace_id", "user_projects"])
    @append_keyword_filter(["topic"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'topic': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.history_mgr.stat_history(query)
