from spaceone.core.error import *


class ERROR_DIFF_TIME_RANGE(ERROR_INVALID_ARGUMENT):
    _message = 'There are less than two items in time, so it cannot be compared. (start = {start}, end = {end})'


class ERROR_NOT_FOUND_DIFF_FIELDS(ERROR_INVALID_ARGUMENT):
    _message = 'The fields is not found. ({field_type} = {fields})'


class ERROR_DIFF_TIME_RANGE(ERROR_INVALID_ARGUMENT):
    _message = "The 'to' time must be greater than the 'from' time."
