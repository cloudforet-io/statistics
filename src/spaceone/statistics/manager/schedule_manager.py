import logging
from typing import Tuple
from mongoengine import QuerySet

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

    def update_schedule_by_vo(self, params: dict, schedule_vo: Schedule) -> Schedule:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_schedule_by_vo._rollback] Revert Data : "
                f'{old_data["schedule_id"]}'
            )
            schedule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, schedule_vo.to_dict())

        return schedule_vo.update(params)

    @staticmethod
    def delete_schedule_by_vo(schedule_vo: Schedule) -> None:
        schedule_vo.delete()

    def get_schedule(self, schedule_id: str, domain_id: str) -> Schedule:
        return self.schedule_model.get(schedule_id=schedule_id, domain_id=domain_id)

    def list_schedules(self, query: dict) -> Tuple[QuerySet, int]:
        return self.schedule_model.query(**query)

    def stat_schedules(self, query: dict) -> dict:
        return self.schedule_model.stat(**query)

    def list_domains(self, query: dict) -> dict:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        return identity_mgr.list_domains(query)
