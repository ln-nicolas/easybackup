
from pathlib import Path
from typing import List

from ..policy.cleanup import CleanupPolicy
from .backup import Backup
from .volume import Volume
from .lexique import ARCHIVE_TYPE
from ..utils.taggable import Taggable


class RepositoryAdapter(Taggable):

    tag_type = False

    _prefix = 'easybackup'

    def __init__(self, **conf):
        self.setup(**conf)

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
        return backup.formated_name

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

    @property
    def adapter(self) -> RepositoryAdapter:
        return self._adapter

    def fetch(self) -> List[Backup]:
        """ return all backups on the repository """
        return self._adapter.fetch_backups()

    def last_backup(self) -> Backup:
        """ return last backup on the repository """
        fetch = self.fetch()
        if not fetch:
            return False
        else:
            return self.fetch()[-1]

    def cleanup(self, policy: CleanupPolicy, volume: Volume = False):
        self.cleanup_backups(self.tocleanup(policy, volume))

    def tocleanup(self, policy: CleanupPolicy, volume: Volume = False) -> List[Backup]:
        backups = self.fetch()
        backups = volume.match(backups) if volume else backups
        tocleanup = policy.filter_backups_to_cleanup(backups)
        return tocleanup

    def cleanup_backups(self, backups: List[Backup]):
        self._adapter.cleanup_backups(backups)
