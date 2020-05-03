import os
import subprocess
import tarfile
import time

import pytest

from easybackup.adapters.local import (LocalBackupCreator,
                                       LocalRepositoryAdapter,
                                       LocalToLocal)
from tests_core.mock import clock
from easybackup.core.backup import Backup

from .utils import temp_directory

mockbackups = [
    'easybackup-myproject-db-20200420_130000.tar',
    'easybackup-myproject-db-20200420_130100.tar',
    'easybackup-myproject-db-20200421_130000.tar',
    'easybackup-myproject-db-20200422_130000.tar'
]


def test_fetch_backup_from_local_repository(temp_directory):

    for backup in mockbackups:
        file = open(temp_directory(backup), 'w+')
        file.write('A'*1000)
        file.close()

    adapter = LocalRepositoryAdapter(directory=temp_directory())
    backups = adapter.fetch_backups()
    assert len(backups) == 4
    assert backups[0].project == 'myproject'
    assert backups[0].volume == 'db'
    assert backups[0].datetime == '20200420_130000'


def test_cleanup_backup_from_local_repository(temp_directory):

    for backup in mockbackups:
        file = open(temp_directory(backup), 'w+')
        file.write('A'*1000)
        file.close()

    adapter = LocalRepositoryAdapter(directory=temp_directory())

    backups = adapter.fetch_backups()
    assert len(backups) == 4

    adapter.cleanup_backups(backups[:2])

    backups = adapter.fetch_backups()
    assert len(backups) == 2
    assert backups[0].datetime == '20200421_130000'
    assert backups[1].datetime == '20200422_130000'


@clock('20200420_130000')
def test_backup_local_file_to_backup_repository(temp_directory):

    random_file = open(temp_directory('random.txt'), "w+")
    random_file.write('A'*1000)
    random_file.close()

    creator = LocalBackupCreator(
        source=temp_directory('random.txt'),
        backup_directory=temp_directory('backups')
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='db',
        datetime='20200420_130000'
    ))

    backups = creator.target_repository.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Test backup extraction
    adapter = creator.target_adapter()
    backup_file = adapter.fetch_backups()

    tar = tarfile.open(adapter.path(adapter.backup_to_filename(backup_file[0])))
    tar.extractall(temp_directory('backups'))
    extract_file = open(temp_directory('backups/random.txt'), "r").read()

    assert extract_file == 'A'*1000


@clock('20200420_130000')
def test_backup_local_directory_to_backup_repository(temp_directory):

    os.mkdir(temp_directory('production'))
    os.mkdir(temp_directory('production/foo'))
    os.mkdir(temp_directory('production/bar'))
    os.mkdir(temp_directory('production/bar/bar'))

    random_file = open(temp_directory('production/bar/bar/random.txt'), "w+")
    random_file.write('A'*1000)
    random_file.close()

    creator = LocalBackupCreator(
        source=temp_directory('production'),
        backup_directory=temp_directory('backups')
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='db',
        datetime='20200420_130000'
    ))

    backups = creator.target_repository.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Test backup extraction
    adapter = creator.target_adapter()
    backup_file = adapter.fetch_backups()

    tar = tarfile.open(adapter.path(adapter.backup_to_filename(backup_file[0])))
    tar.extractall(temp_directory('backups'))

    assert os.path.isdir(temp_directory('backups/production'))
    assert os.path.isdir(temp_directory('backups/production/foo'))
    assert os.path.isdir(temp_directory('backups/production/bar'))
    assert os.path.isdir(temp_directory('backups/production/bar/bar'))
    assert os.path.isfile(temp_directory('backups/production/bar/bar/random.txt'))

    extract_file = open(temp_directory('backups/production/bar/bar/random.txt'), "r").read()
    assert extract_file == 'A'*1000


def test_copy_backup_from_folder_to_an_other(temp_directory):

    random_file = open(temp_directory('random.txt'), "w+")
    random_file.write('A'*1000)
    random_file.close()
    os.mkdir(temp_directory('backups_copy'))

    creator = LocalBackupCreator(
        source=temp_directory('random.txt'),
        backup_directory=temp_directory('backups')
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='db',
        datetime='20200420_130000'
    ))

    rep_backups = LocalRepositoryAdapter(directory=temp_directory('backups'))
    rep_backups_copy = LocalRepositoryAdapter(directory=temp_directory('backups_copy'))

    link = LocalToLocal(
        source_directory=temp_directory('backups'),
        target_directory=temp_directory('backups_copy')
    )

    backup = rep_backups.fetch_backups()[0]
    link.copy_backup(backup)

    backups = rep_backups_copy.fetch_backups()
    assert len(backups) == 1
    assert backups[0].datetime == backup.datetime

    # Test backup extraction
    tar = tarfile.open(rep_backups_copy.path(str(backup)))
    tar.extractall(temp_directory('backups'))
    extract_file = open(temp_directory('backups/random.txt'), "r").read()

    assert extract_file == 'A'*1000
