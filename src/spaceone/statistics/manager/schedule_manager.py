import logging

from spaceone.core.manager import BaseManager
from spaceone.statistics.model.schedule_model import Schedule
from spaceone.statistics.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


class ScheduleManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schedule_model: Schedule = self.locator.get_model("Schedule")

    def add_schedule(self, params: dict) -> Schedule:
        def _rollback(vo: Schedule) -> None:
            _LOGGER.info(
                f"[add_schedule._rollback] "
                f"Delete schedule : {vo.topic} "
                f"({vo.schedule_id})"
            )
            vo.delete()

        schedule_vo: Schedule = self.schedule_model.create(params)
        self.transaction.add_rollback(_rollback, schedule_vo)

        return schedule_vo

    def update_schedule(self, params: dict) -> Schedule:
        schedule_vo: Schedule = self.get_schedule(
            params["schedule_id"], params["domain_id"]
        )
        return self.update_schedule_by_vo(params, schedule_vo)

    def update_schedule_by_vo(self, params: dict, schedule_vo: Schedule):
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_schedule_by_vo._rollback] Revert Data : "
                f'{old_data["schedule_id"]}'
            )
            schedule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, schedule_vo.to_dict())

        return schedule_vo.update(params)

    def delete_schedule(self, schedule_id: str, domain_id: str) -> None:
        schedule_vo: Schedule = self.get_schedule(schedule_id, domain_id)
        schedule_vo.delete()

    def get_schedule(
        self,
        schedule_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
        only: list = None,
    ) -> Schedule:
        conditions = {"schedule_id": schedule_id, "domain_id": domain_id}

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        if only:
            conditions["only"] = only

        return self.schedule_model.get(**conditions)

    def list_schedules(self, query: dict) -> dict:
        return self.schedule_model.query(**query)

    def stat_schedules(self, query: dict) -> dict:
        return self.schedule_model.stat(**query)

    def list_domains(self, query: dict) -> dict:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        return identity_mgr.list_domains(query)
