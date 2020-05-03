from typing import List

from ..core.backup import Backup
from ..core.clock import Clock


class CleanupPolicy():

    def filter_backups_to_cleanup(self, backups: List[Backup]) -> (List[Backup]):
        """ Return a tuple backup to cleanup """
        pass

    def filter_backups_to_keep(self, backups: List[Backup]) -> (List[Backup]):
        """ Return a tuple backup to keep"""
        pass


class ClearAllCleanupPolicy(CleanupPolicy):

    def filter_backups_to_cleanup(self, backups):
        return backups

    def filter_backups_to_keep(self, backups):
        return []


class LifetimeCleanupPolicy(CleanupPolicy):

    def __init__(self, max_age: int, mininum: int):
        self._max_age = max_age
        self._minimum = mininum

    def filter_backups_to_cleanup(self, backups: List[Backup]) -> List[Backup]:
        tokeep = self.filter_backups_to_keep(backups)
        return list(filter(
            lambda backup: backup not in tokeep,
            backups
        ))

    def filter_backups_to_keep(self, backups: List[Backup]) -> List[Backup]:

        minimal = self.minimal_backups(backups)
        valids = self.filter_valid_backups(backups)

        if len(valids) > len(minimal):
            return valids
        else:
            return minimal

    @property
    def max_age(self) -> int:
        return self._max_age

    @property
    def minimum(self) -> int:
        return self._minimum

    def filter_valid_backups(self, backups: List[Backup]) -> List[Backup]:
        valids = list(filter(
            lambda backup: not self.outdated(backup.datetime),
            backups
        ))
        return valids

    def minimal_backups(self, backups: List[Backup]) -> List[Backup]:
        if self._minimum <= 0:
            return []
        else:
            backups = sorted(
                backups,
                key=lambda backup: backup.datetime
            )
            return backups[-self.minimum:]

    def outdated(self, date: str) -> bool:
        backup_age = Clock.delta_from(date)
        return backup_age > self.max_age
