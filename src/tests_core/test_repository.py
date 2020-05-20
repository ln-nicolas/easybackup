# -*- coding: utf-8 -*-
import pytest

from easybackup.core.backup import Backup
from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.core.repository import Repository

from .mock import MemoryBackupCreator, MemoryRepositoryAdapter, clock

__author__ = "sne3ks"
__copyright__ = "sne3ks"
__license__ = "mit"

mockbackups = [
    'easybackup-myproject-db-20200420_130000.tar',
    'easybackup-myproject-db-20200420_130100.tar',
    'easybackup-myproject-db-20200421_130000.tar',
    'easybackup-myproject-db-20200422_130000.tar',
]

memory_backup_creator = MemoryBackupCreator()


@pytest.fixture
def memory_adapter():
    return MemoryRepositoryAdapter(backups=mockbackups)


@clock('20200422_130000')
def test_load_backups_repository(memory_adapter):

    rep = Repository(adapter=memory_adapter)
    backups = rep.fetch()

    assert len(backups) == 4
    assert type(backups[0]) is Backup
    assert backups[0].datetime == '20200420_130000'
    assert backups[0].volume == 'db'
    assert backups[0].project == 'myproject'
    assert backups[0].file_type == 'tar'


def test_fetch_backup_on_repository_with_many_volumes():

    mockbackups = [
        'easybackup-myproject-db-20200420_130000.tar',
        'easybackup-myproject-db-20200420_130100.tar',
        'easybackup-myproject-app-20200420_130000.tar',
        'easybackup-myproject-app-20200420_130100.tar',
    ]
    memory_adapter = MemoryRepositoryAdapter(backups=mockbackups)

    repository = Repository(adapter=memory_adapter)
    myproject_db_composer = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=False,
        repository=repository,
        cleanup_policy=False,
        backup_policy=False
    )

    backups = myproject_db_composer.fetch()
    assert len(backups) == 2
    assert backups[0].volume == 'db'
    assert backups[1].volume == 'db'


def test_fetch_backup_on_repository_with_many_project():

    mockbackups = [
        'easybackup-myproject-db-20200420_130000.tar',
        'easybackup-myproject-db-20200420_130100.tar',
        'easybackup-yourproject-db-20200420_130000.tar',
        'easybackup-yourproject-db-20200420_130100.tar',
    ]
    memory_adapter = MemoryRepositoryAdapter(backups=mockbackups)

    repository = Repository(adapter=memory_adapter)
    myproject_db_composer = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=False,
        repository=repository,
        cleanup_policy=False,
        backup_policy=False
    )

    backups = myproject_db_composer.fetch()
    assert len(backups) == 2
    assert backups[0].project == 'myproject'
    assert backups[1].project == 'myproject'
