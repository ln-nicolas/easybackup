import datetime
from .lexique import DATE_FORMAT


class Clock():

    _now = False

    @classmethod
    def monkey_now(cls, now):
        cls._now = now

    @classmethod
    def now(cls):
        if cls._now:
            return cls._now
        else:
            now = datetime.datetime.now()
            return now.strftime(DATE_FORMAT)

    @classmethod
    def delta_from(cls, date_from: str):
        return cls.delta(date_from, cls.now())

    @classmethod
    def delta(cls, date_from: str, date_to: str):
        date_from = datetime.datetime.strptime(date_from, DATE_FORMAT)
        date_to = datetime.datetime.strptime(date_to, DATE_FORMAT)
        return (date_to-date_from).total_seconds()
