import logging
import copy

from spaceone.core.service import *

from spaceone.statistics.error import *
from spaceone.statistics.manager.resource_manager import ResourceManager
from spaceone.statistics.manager.schedule_manager import ScheduleManager
from spaceone.statistics.model import Schedule

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ScheduleService(BaseService):
    resource = "Schedule"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_mgr: ResourceManager = self.locator.get_manager("ResourceManager")
        self.schedule_mgr: ScheduleManager = self.locator.get_manager("ScheduleManager")

    @transaction(
        permission="statistics:Schedule.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["topic", "options", "schedule", "domain_id"])
    def add(self, params: dict) -> Schedule:
        """Add schedule for statistics

        Args:
            params (dict): {
                'topic': 'str',      # required
                'options': 'dict',   # required
                'schedule': 'dict',  # required
                'tags': 'dict',
                'domain_id': 'str'   # injected from auth (required)
            }

        Returns:
            schedule_vo
        """

        domain_id = params["domain_id"]
        options = copy.deepcopy(params["options"])
        schedule = params["schedule"]

        self._check_schedule(schedule)
        self._verify_query_option(options)
        return self.schedule_mgr.add_schedule(schedule)

    @transaction(
        permission="statistics:Schedule.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["schedule_id", "domain_id"])
    def update(self, params: dict) -> Schedule:
        """Update schedule

        Args:
            params (dict): {
                'schedule_id': 'str',   # required
                'schedule': 'dict',
                'tags': 'dict',
                'domain_id': 'str'      # injected from auth (required)
            }

        Returns:
            schedule_vo
        """
        self._check_schedule(params.get("schedule"))

        schedule_vo = self.schedule_mgr.get_schedule(
            params["schedule_id"], params["domain_id"]
        )

        return self.schedule_mgr.update_schedule_by_vo(params, schedule_vo)

    @transaction(
        permission="statistics:Schedule.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["schedule_id", "domain_id"])
    def enable(self, params):
        """Enable schedule

        Args:
            params (dict): {
                'schedule_id': 'str',     # required
                'domain_id': 'str'        # injected from auth (required)
            }

        Returns:
            schedule_vo
        """

        domain_id = params["domain_id"]
        schedule_id = params["schedule_id"]

        schedule_vo = self.schedule_mgr.get_schedule(schedule_id, domain_id)
        return self.schedule_mgr.update_schedule_by_vo(
            {"state": "ENABLED"}, schedule_vo
        )

    @transaction(
        permission="statistics:Schedule.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["schedule_id", "domain_id"])
    def disable(self, params):
        """Disable schedule

        Args:
            params (dict): {
                'schedule_id': 'str',     # required
                'domain_id': 'str'        # injected from auth (required)
            }

        Returns:
            schedule_vo
        """

        domain_id = params["domain_id"]
        schedule_id = params["schedule_id"]

        schedule_vo = self.schedule_mgr.get_schedule(schedule_id, domain_id)
        return self.schedule_mgr.update_schedule_by_vo(
            {"state": "DISABLED"}, schedule_vo
        )

    @transaction(
        permission="statistics:Schedule.write",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["schedule_id", "domain_id"])
    def delete(self, params):
        """Delete schedule

        Args:
            params (dict): {
                'schedule_id': 'str',     # required
                'domain_id': 'str'        # injected from auth (required)
            }

        Returns:
            None
        """

        schedule_vo = self.schedule_mgr.get_schedule(
            params["schedule_id"], params["domain_id"]
        )
        self.schedule_mgr.delete_schedule_by_vo(schedule_vo)

    @transaction(
        permission="statistics:Schedule.read",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["schedule_id", "domain_id"])
    def get(self, params):
        """Get schedule

        Args:
            params (dict): {
                'schedule_id': 'str',     # required
                'domain_id': 'str'        # injected from auth (required)
            }

        Returns:
            schedule_vo
        """

        schedule_id = params["schedule_id"]
        domain_id = params["domain_id"]

        return self.schedule_mgr.get_schedule(schedule_id, domain_id)

    @transaction(
        permission="statistics:Schedule.read",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "schedule_id",
            "topic",
            "state",
            "domain_id",
        ]
    )
    @append_keyword_filter(["schedule_id", "topic"])
    def list(self, params):
        """List schedules

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'schedule_id': 'str',
                'topic': 'str',
                'state': 'str',
                'domain_id': 'str',                            # injected from auth (required)
            }

        Returns:
            schedule_vos (object)
            total_count
        """

        query = params.get("query", {})
        return self.schedule_mgr.list_schedules(query)

    @transaction(
        permission="statistics:Schedule.read",
        role_types=["DOMAIN_ADMIN"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["domain_id"])
    @append_keyword_filter(["schedule_id", "topic"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'  # required
                'domain_id': 'str',                                     # injected from auth (required)
            }

        Returns:
            dict: {
                'results': 'list',
                'total_count': 'int'
            }

        """

        query = params.get("query", {})
        return self.schedule_mgr.stat_schedules(query)

    @transaction(exclude=["authentication", "authorization", "mutation"])
    @append_query_filter([])
    def list_domains(self, params):
        """This is used by Scheduler
        Returns:
            results (list)
            total_count (int)
        """
        mgr = self.locator.get_manager("ScheduleManager")
        query = params.get("query", {})
        result = mgr.list_domains(query)
        return result

    @staticmethod
    def _check_schedule(schedule: dict) -> None:
        if schedule and len(schedule) > 1:
            raise ERROR_SCHEDULE_OPTION()

    def _verify_query_option(self, options: dict) -> None:
        aggregate = options.get("aggregate", [])
        page = options.get("page", {})

        self.resource_mgr.stat(aggregate, page)
