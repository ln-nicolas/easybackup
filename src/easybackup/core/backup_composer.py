from typing import List

from .backup import Backup, Volume
from .backup_creator import BackupCreator
from ..policy.backup import BackupPolicy
from ..policy.synchronization import CopyPastePolicy
from ..policy.cleanup import CleanupPolicy
from .clock import Clock
from .repository import Repository


class BackupComposer():

    def __init__(
        self,
        project: str,
        volume: str,
        creator: BackupCreator,
        repository: Repository = False,
        repositories: List[Repository] = False,
        synchronizeers: List[BackupCreator] = [],
        cleanup_policy: CleanupPolicy = False,
        backup_policy: BackupPolicy = False
    ):
        self._project = project
        self._volume = volume
        self._creator = creator

        if repository:
            self._repository = repository
        else:
            self._repository = creator.target_repository

        self._repositories = repositories

        self._synchronizeers = synchronizeers
        self._cleanup_policy = cleanup_policy
        self._backup_policy = backup_policy

    @property
    def project(self) -> str:
        return self._project

    @property
    def volume(self) -> str:
        return self._volume

    @property
    def creator(self) -> BackupCreator:
        return self._creator

    @property
    def repository(self) -> Repository:
        return self._repository

    @property
    def synchronizeers(self) -> List[BackupCreator]:
        return self._synchronizeers

    @property
    def cleanup_policy(self) -> CleanupPolicy:
        return self._cleanup_policy

    @property
    def backup_policy(self) -> BackupPolicy:
        return self._backup_policy

    def fetch(self):
        backups = self.repository.fetch()
        return list(filter(
            lambda backup: backup.project == self.project and backup.volume == self.volume,
            backups
        ))

    def build_backup(self):
        self._creator.build_backup(Backup(
            datetime=Clock.now(),
            volume=self.volume,
            project=self.project
        ))
        for synchronizeer in self.synchronizeers:
            synchronizeer.synchronize()

    def run(self):
        if self.cleanup_policy:
            self.run_cleanup()

        if self.backup_policy:
            self.run_backup()

    def run_backup(self):

        backups = self.repository.fetch()
        should_backup = self._backup_policy.should_backup(backups)
        if should_backup:
            self.build_backup()

    def run_cleanup(self):
        self.repository.cleanup(policy=self.cleanup_policy, volume=Volume(self.volume, self.project))
