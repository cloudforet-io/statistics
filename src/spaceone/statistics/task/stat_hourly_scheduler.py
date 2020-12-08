import datetime
import logging

import consul
import time
from celery import shared_task

from spaceone.core import config
from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.core.celery.tasks import BaseSchedulerTask
from spaceone.core.locator import ERROR_CONFIGURATION, Locator

__all__ = ['StatHourlyScheduler']

_LOGGER = logging.getLogger(__name__)


def _get_domain_id_from_token(token):
    decoded_token = JWTUtil.unverified_decode(token)
    return decoded_token['did']


WAIT_QUEUE_INITIALIZED = 10  # seconds for waiting queue initialization
INTERVAL = 10
MAX_COUNT = 10


class Consul:
    def __init__(self, config):
        """
        Args:
          - config: connection parameter

        Example:
            config = {
                    'host': 'consul.example.com',
                    'port': 8500
                }
        """
        self.config = self._validate_config(config)

    def _validate_config(self, config):
        """
        Parameter for Consul
        - host, port=8500, token=None, scheme=http, consistency=default, dc=None, verify=True, cert=None
        """
        options = ['host', 'port', 'token', 'scheme', 'consistency', 'dc', 'verify', 'cert']
        result = {}
        for item in options:
            value = config.get(item, None)
            if value:
                result[item] = value
        return result

    def patch_token(self, key):
        """
        Args:
            key: Query key (ex. /debug/supervisor/TOKEN)

        """
        try:
            conn = consul.Consul(**self.config)
            index, data = conn.kv.get(key)
            return data['Value'].decode('ascii')

        except Exception as e:
            _LOGGER.debug(f'[patch_token] failed: {e}')
            return False


def _validate_token(token):
    if isinstance(token, dict):
        protocol = token['protocol']
        if protocol == 'consul':
            consul_instance = Consul(token['config'])
            value = False
            while value is False:
                uri = token['uri']
                value = consul_instance.patch_token(uri)
                if value:
                    _LOGGER.warn(f'[_validate_token] token: {value[:30]} uri: {uri}')
                    break
                _LOGGER.warn(f'[_validate_token] token is not found ... wait')
                time.sleep(INTERVAL)

            token = value
    return token


class StatHourlyScheduler():
    def __init__(self, interval=1, ):
        self.config = self.parse_config(interval)

        self.count = self._init_count()
        self.locator = Locator()
        self.TOKEN = self._update_token()
        self.domain_id = _get_domain_id_from_token(self.TOKEN)

    def parse_config(self, expr):
        """ expr
          format: integer (hour)
        """

        if not isinstance(expr, int):
            _LOGGER.error(f'[parse_config] Wrong configuration')
            raise ERROR_CONFIGURATION(key='interval')
        return expr

    def _init_count(self):
        # get current time
        cur = datetime.datetime.utcnow()
        count = {
            'previous': cur,  # Last check_count time
            'index': 0,  # index
            'hour': cur.hour,  # previous hour
            'started_at': 0,  # start time of push_token
            'ended_at': 0  # end time of execution in this tick
        }
        _LOGGER.debug(f'[_init_count] {count}')
        return count

    def _update_token(self):
        token = config.get_global('TOKEN')
        if token == "":
            token = _validate_token(config.get_global('TOKEN_INFO'))
        return token

    def create_task(self):
        domains = self.list_domains()
        result = []
        for domain in domains:
            stp = self._create_job_request(domain)
            result.append(stp)
        return result

    def list_domains(self):
        try:
            ok = self.check_count()
            if ok == False:
                # ERROR LOGGING
                pass
            # Loop all domain, then find schedule
            metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
            schedule_svc = self.locator.get_service('ScheduleService', metadata)
            params = {}
            resp = schedule_svc.list_domains(params)
            _LOGGER.debug(f'[list_domain] num of domains: {resp["total_count"]}')
            return resp['results']
        except Exception as e:
            _LOGGER.error(e)
            return []

    def check_count(self):
        # check current count is correct or not
        cur = datetime.datetime.utcnow()
        hour = cur.hour
        # check
        if (self.count['hour'] + self.config) % 24 != hour:
            if self.count['hour'] == hour:
                _LOGGER.error('[check_count] duplicated call in the same time')
            else:
                _LOGGER.error('[check_count] missing time')

        # This is continuous task
        count = {
            'previous': cur,
            'index': self.count['index'] + 1,
            'hour': hour,
            'started_at': cur
        }
        self.count.update(count)

    def _update_count_ended_at(self):
        cur = datetime.datetime.utcnow()
        self.count['ended_at'] = cur

    def _list_schedule(self, hour, domain_id):
        """ List statistics.Schedule
        """
        params = {
            'query': {
                'filter': [{'k': 'schedule.hours', 'v': hour, 'o': 'eq'}],
            },
            'domain_id': domain_id
        }
        metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
        schedule_svc = self.locator.get_service('ScheduleService', metadata)
        schedules, total_count = schedule_svc.list(params)
        _LOGGER.debug(f'[_list_schedule] schedules: {schedules}, total_count: {total_count}')
        return schedules

    def _create_job_request(self, domain):
        """ Based on domain, create Job Request

        Returns:
            jobs: SpaceONE Pipeline Template
        """
        _LOGGER.debug(f'[_create_job_request] domain: {domain}')
        metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
        schedules = self._list_schedule(self.count['hour'], domain['domain_id'])
        sched_jobs = []
        for schedule in schedules:
            sched_job = {
                'locator': 'SERVICE',
                'name': 'HistoryService',
                'metadata': metadata,
                'method': 'create',
                'params': {
                    'params': {'schedule_id': schedule.schedule_id, 'domain_id': domain['domain_id']}
                }
            }
            sched_jobs.append(sched_job)

        stp = {
            'name': 'statistics_hourly_schedule',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': sched_jobs
        }
        _LOGGER.debug(f'[_create_job_request] tasks: {stp}')
        return stp

    @staticmethod
    def _create_schedule_params(schedule, domain_id):
        dict_schedule = dict(schedule.to_dict())
        _LOGGER.debug(f'[_create_schedule_params] schedule: {schedule}')

        required_params = ['schedule_id', 'data_source_id', 'resource_type', 'query', 'join', 'formulas', 'domain_id']
        result = {'schedule_id': dict_schedule['schedule_id'], 'domain_id': domain_id}
        print('#' * 30)
        for param in required_params:
            print(f'check : {param}')
            if param in dict_schedule['options']:
                result[param] = dict_schedule['options'][param]
        _LOGGER.debug(f'[_create_schedule_params] params: {result}')
        return result


@shared_task(base=BaseSchedulerTask,bind=True )
def stat_hour_scheduler(self):
    return StatHourlyScheduler().create_task()
