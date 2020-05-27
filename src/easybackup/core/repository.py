
from pathlib import Path
from typing import List

from ..policy.cleanup import CleanupPolicy
from ..utils.taggable import Taggable
from .backup import Backup
from .hook import Hook
from .lexique import ARCHIVE_TYPE
from .volume import Volume


class RepositoryAdapter(Taggable):

    type_tage = False

    _prefix = 'easybackup'

    def __init__(self, **conf):
        self.setup(**conf)

    def __str__(self):
        return "[%s repository]" % (self.type_tag or "")

    def setup(self, **conf):
        """ setup the adapter with config values """
        raise NotImplementedError

    def fetch_backups(self) -> List[Backup]:
        """ return a list of string describing backup from the repository """
        raise NotImplementedError

    def cleanup_backups(self, backups: List[Backup]):
        """ cleanup backups """
        raise NotImplementedError

    @classmethod
    def backup_to_filename(cls, backup: Backup) -> str:
        return backup.formated_name +'.'+ backup.file_type

    @classmethod
    def filename_to_backup(cls, filename: str) -> Backup:
        _, projet, volume, date = filename.split('-')
        date, file_type = date.split('.')
        return Backup(**{
            'volume': volume,
            'project': projet,
            'datetime': date,
            'file_type': file_type
        })

    @classmethod
    def filename_match_backup(cls, filename: str) -> bool:

        if not filename.startswith(cls._prefix):
            return False

        if not (Path(filename).suffix.split('.')[-1] in ARCHIVE_TYPE):
            return False

        return True


class Repository():

    def __init__(self, adapter, name=False):
        self._adapter = adapter
        self.name = name

    def __str__(self):
        return str(self.adapter)

    @property
    def adapter(self) -> RepositoryAdapter:
        return self._adapter

    def fetch(self, volume: Volume = False) -> List[Backup]:
        """ return all backups on the repository """
        Hook.plays('before_fetch_backups', repository=self, volume=volume)
        backups = self._adapter.fetch_backups()

        if volume:
            backups = volume.match(backups)

        Hook.plays('after_fetch_backups', repository=self, backups=backups, volume=volume)
        return backups

    def last_backup(self) -> Backup:
        """ return last backup on the repository """
        fetch = self.fetch()
        if not fetch:
            return False
        else:
            return self.fetch()[-1]

    def cleanup(self, policy: CleanupPolicy, volume: Volume = False):
        tocleanup = self.tocleanup(policy, volume)
        self.cleanup_backups(tocleanup)

        Hook.plays(
            'on_cleanup_backups',
            volume=volume,
            tocleanup=tocleanup,
            policy=policy
        )

    def tocleanup(self, policy: CleanupPolicy, volume: Volume = False) -> List[Backup]:
        backups = self.fetch()
        backups = volume.match(backups) if volume else backups
        tocleanup = policy.filter_backups_to_cleanup(backups)
        return tocleanup

    def cleanup_backups(self, backups: List[Backup]):
        self._adapter.cleanup_backups(backups)
