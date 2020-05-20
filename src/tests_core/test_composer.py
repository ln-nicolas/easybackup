# -*- coding: utf-8 -*-

from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.core.repository import Repository
from easybackup.policy.backup import TimeIntervalBackupPolicy

from .mock import MemoryBackupCreator, MemoryRepositoryAdapter, clock

__author__ = "sne3ks"
__copyright__ = "sne3ks"
__license__ = "mit"

mockbackups = [
    'easybackup-myproject-db-20200420_130000-134234234.tar',
    'easybackup-myproject-db-20200420_130100-423423442.tar',
    'easybackup-myproject-db-20200421_130000-134234234.tar',
    'easybackup-myproject-db-20200422_130000-423423442.tar',
]

memory_backup_creator = MemoryBackupCreator()


@clock('20200422_130000')
def test_can_know_the_backup_target_repository_from_creator():

    creator = MemoryBackupCreator()

    repository = creator.target_repository
    assert type(repository.adapter) is MemoryRepositoryAdapter
    assert repository.adapter.bucket == memory_backup_creator.target_bucket


def test_composer_can_infer_repository_from_creator():

    repository = Repository(adapter=MemoryRepositoryAdapter(bucket='C', force_clear=True))
    creator = MemoryBackupCreator(target_bucket='C')
    backup_policy = TimeIntervalBackupPolicy(1000)

    composer = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=creator,
        cleanup_policy=False,
        backup_policy=backup_policy
    )

    backups = repository.fetch()
    assert len(backups) == 0

    composer.run()

    backups = repository.fetch()
    assert len(backups) == 1
