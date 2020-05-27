from typing import List

from ..core.backup import Backup
from ..core.clock import Clock
from ..utils.taggable import Taggable


class BackupPolicy(Taggable):

    def __str__(self):
        return "[%s backup policy]" % (self.type_tag or "")

    def should_backup(self, backups: List[Backup]) -> bool:
        """ Determine if a new backup should be done according to existing backups """


class TimeIntervalBackupPolicy(BackupPolicy):

    type_tag = 'timeinterval'

    def __init__(self, interval: int):
        self._interval = interval

    @property
    def interval(self) -> int:
        return self._interval

    def should_backup(self, backups: List[Backup]) -> bool:

        if not backups:
            return True

        ordered = list(sorted(
            backups,
            key=lambda backup: backup.datetime
        ))

        lastdatetime = ordered[-1].datetime
        delta_without_backup = Clock.delta_from(lastdatetime)

        return delta_without_backup >= self.interval
