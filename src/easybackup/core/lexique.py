import re
from ..i18n import i18n

ARCHIVE_TYPE = [
    'zip',
    'tar',
    'sql'
]

DATE_FORMAT = '%Y%m%d_%H%M%S'


def is_number_version(string):
    return re.compile('[0-9]+\.[0-9]+\.[0-9]+').match(string)


def parse_time_duration(string):

    if not re.compile('^((([0-9]*)d)?(([0-9]*)h)?(([0-9]*)m)?(([0-9]*)s)?)$|^([0-9]*)$').match(string):
        raise ValueError(i18n('time_do_not_match_time_format', string=string))

    if re.compile('^[0-9]*$').match(string):
        return int(string)

    seconds = 0
    re_seconds = re.findall('([0-9]*)s', string)
    if re_seconds:
        seconds = int(re_seconds[0])

    minutes = 0
    re_minutes = re.findall('([0-9]*)m', string)
    if re_minutes:
        minutes = int(re_minutes[0])*60

    hours = 0
    re_hours = re.findall('([0-9]*)h', string)
    if re_hours:
        hours = int(re_hours[0])*3600

    days = 0
    re_days = re.findall('([0-9]*)d', string)
    if re_days:
        days = int(re_days[0])*3600*24

    return seconds + minutes + hours + days
