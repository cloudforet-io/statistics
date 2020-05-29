from spaceone.core.error import *


class ERROR_SCHEDULE_OPTION(ERROR_INVALID_ARGUMENT):
    _message = 'Only one schedule option can be set. (cron | interval | minutes | hours)'
