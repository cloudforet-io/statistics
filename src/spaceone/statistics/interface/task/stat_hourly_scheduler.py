import logging
from datetime import datetime

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler

__all__ = ["StatHourlyScheduler"]

_LOGGER = logging.getLogger(__name__)


class StatHourlyScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=":00"):
        super().__init__(queue, interval, minute)
        self.locator = Locator()
        self._init_config()

    def _init_config(self):
        self._token = config.get_global("TOKEN")
        if self._token is None:
            raise ERROR_CONFIGURATION(key="TOKEN")

    def create_task(self):
        current_hour = datetime.utcnow().hour
        result = []
        for domain_info in self.list_domains():
            stp = self._create_job_request(domain_info["domain_id"], current_hour)
            result.append(stp)
        return result

    def list_domains(self):
        try:
            schedule_svc = self.locator.get_service(
                "ScheduleService", {"token": self._token}
            )
            response = schedule_svc.list_domains({})
            return response.get("results", [])
        except Exception as e:
            _LOGGER.error(e)
            return []

    def _list_schedule(self, domain_id: str, current_hour: int):
        params = {
            "query": {
                "filter": [{"k": "schedule.hours", "v": current_hour, "o": "eq"}],
            },
            "domain_id": domain_id,
        }
        schedule_svc = self.locator.get_service(
            "ScheduleService", {"token": self._token}
        )
        schedules, total_count = schedule_svc.list(params)
        _LOGGER.debug(
            f"[_list_schedule] scheduled count (UTC {current_hour}): {total_count}"
        )
        return schedules

    def _create_job_request(self, domain_id: str, current_hour: int):
        _LOGGER.debug(f"[_create_job_request] domain: {domain_id}")
        schedules = self._list_schedule(domain_id, current_hour)
        schedule_jobs = []
        for schedule in schedules:
            job = {
                "locator": "SERVICE",
                "name": "HistoryService",
                "metadata": {
                    "token": self._token,
                },
                "method": "create",
                "params": {
                    "params": {
                        "schedule_id": schedule.schedule_id,
                        "domain_id": domain_id,
                    }
                },
            }
            schedule_jobs.append(job)

        stp = {
            "name": "statistics_hourly_schedule",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": schedule_jobs,
        }
        _LOGGER.debug(f"[_create_job_request] tasks: {stp}")
        return stp
