# -*- coding: utf-8 -*-
import random
import string
import tarfile
from ftplib import FTP
from contextlib import contextmanager
import pytest
import os
from easybackup.adapters.ftp import FtpRepositoryAdapter, LocalToFtp, FtpToLocal
from easybackup.adapters.local import (LocalBackupCreator,
                                       LocalRepositoryAdapter)
from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.policy.backup import TimeIntervalBackupPolicy
from easybackup.core.repository import Repository
from easybackup.policy.synchronization import CopyPastePolicy
from easybackup.policy.cleanup import ClearAllCleanupPolicy
from easybackup.core.backup import Volume
from tests_core.mock import clock

from .utils import temp_directory

__author__ = "sne3ks"
__copyright__ = "sne3ks"
__license__ = "mit"

exec(open('./.env').read(), globals())

FTP_CONF = {
    'host': FTP_HOST,
    'user': FTP_USER,
    'password': FTP_PASSWORD,
    'directory' :FTP_BACKUP_DIRECTORY,
    'prefix': 'backup'
}


def randomfile(path):
    content = ''.join(random.choice(string.ascii_lowercase) for i in range(10000))
    file = open(path, 'w+')
    file.write(content)
    file.close()
    return content


@contextmanager
def assert_backup_and_restore(source_file, archive_file):

    file_content = randomfile(source_file)

    yield True

    tar = tarfile.open(archive_file)
    tar.extractall(os.path.dirname(archive_file))
    with open(source_file, "r") as extract:
        assert extract.read() == file_content


@clock('20200102_120000')
def test_backup_then_restore_local_file_to_ftp_repository(temp_directory):

    with assert_backup_and_restore(
        source_file=temp_directory('production.txt'),
        archive_file=temp_directory('restore/easybackup-myproject-db-20200102_120000.tar')
    ):

        local_creator = LocalBackupCreator(
            source=temp_directory('production.txt'),
            backup_directory=temp_directory('backups')
        )

        ftp_adapter = FtpRepositoryAdapter(ftp_conf=FTP_CONF)

        composer = BackupSupervisor(
            project='myproject',
            volume='db',
            creator=local_creator,
            repository=local_creator.target_repository,
            cleanup_policy=False,
            backup_policy=TimeIntervalBackupPolicy(10)
        )

        composer.run()

        link = LocalToFtp(
            source=local_creator.target_adapter(),
            target=ftp_adapter
        )
        link.synchronize()

        target = link.target_adapter
        backups = target.fetch_backups()

        backup = backups[0]

        local_restore = LocalRepositoryAdapter(directory=temp_directory('restore'))

        link = FtpToLocal(
            source=ftp_adapter,
            target=local_restore
        )
        link.copy_backup(backup)

    backups = link.source_repository.fetch()
    assert len(backups) > 0

    link.source_repository.cleanup(policy=ClearAllCleanupPolicy())

    backups = link.source_repository.fetch()
    assert len(backups) == 0


@clock('20200102_120000')
def test_delete_backup_on_ftp_repository(temp_directory):

    randomfile(temp_directory('production-A.txt'))
    randomfile(temp_directory('production-B.txt'))
    ftp_adapter = FtpRepositoryAdapter(ftp_conf=FTP_CONF)

    local_creator_A = LocalBackupCreator(
        source=temp_directory('production-A.txt'),
        backup_directory=temp_directory('backups')
    )
    local_creator_B = LocalBackupCreator(
        source=temp_directory('production-B.txt'),
        backup_directory=temp_directory('backups-twine')
    )

    composerA = BackupSupervisor(
        project='myprojectA',
        volume='db',
        creator=local_creator_A,
        repository=local_creator_A.target_repository,
        cleanup_policy=False,
        backup_policy=TimeIntervalBackupPolicy(10)
    )

    composerB = BackupSupervisor(
        project='myprojectB',
        volume='db',
        creator=local_creator_B,
        repository=local_creator_B.target_repository,
        cleanup_policy=ClearAllCleanupPolicy(),
        backup_policy=TimeIntervalBackupPolicy(10)
    )

    # Backup
    composerA.run()
    composerB.run()

    assert len(composerA.fetch()) == 1
    assert len(composerB.fetch()) == 1

    link = LocalToFtp(
        source=local_creator_A.target_adapter(),
        target=ftp_adapter
    )
    link.synchronize()

    link = LocalToFtp(
        source=local_creator_B.target_adapter(),
        target=ftp_adapter
    )
    link.synchronize()

    # Clear
    rep = Repository(adapter=ftp_adapter)
    rep.cleanup_backups(local_creator_A.target_adapter().fetch_backups())
    backups = rep.fetch()

    assert len(Volume('db', 'myprojectA').match(backups)) == 0
    assert len(Volume('db', 'myprojectB').match(backups)) == 1
