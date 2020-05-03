# -*- coding: utf-8 -*-

import pytest

from easybackup.core import exceptions as exp
from easybackup.core.backup import Backup
from easybackup.core.backup_composer import BackupComposer
from easybackup.core.repository_link import RepositoryLink
from easybackup.core.repository import Repository
from easybackup.policy.backup import TimeIntervalBackupPolicy
from easybackup.policy.synchronization import CopyPastePolicy, MovePolicy

from .mock import (MemoryBackupCreator, MemoryRepositoryAdapter,
                   MemoryRepositoryLink, MockLocalToFtp, MockMysqlToLocal)

__author__ = "sne3ks"
__copyright__ = "sne3ks"
__license__ = "mit"

mockbackups = [
    'easybackup-myproject-db-20200420_130000.tar',
    'easybackup-myproject-db-20200420_130100.tar',
    'easybackup-myproject-db-20200421_130000.tar',
    'easybackup-myproject-db-20200422_130000.tar',
]


def test_compute_backup_to_copy_with_synchronize_all_policy():
    policy = CopyPastePolicy()

    # All to sync
    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', force_clear=True))

    tosynchronize = policy.to_copy(repository_a, repository_b)
    assert len(tosynchronize) == 4

    # Nothing to sync
    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=mockbackups))

    tosynchronize = policy.to_copy(repository_a, repository_b)
    assert len(tosynchronize) == 0

    # Some many to sync
    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=mockbackups[:2]))

    tosynchronize = policy.to_copy(repository_a, repository_b)
    assert len(tosynchronize) == 2


def test_copy_backups_with_syncall_policy():
    policy = CopyPastePolicy()

    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=mockbackups[:2]))

    link = MemoryRepositoryLink(
        source_bucket='A',
        target_bucket='B'
    )

    assert len(repository_b.fetch()) == 2

    link.synchronize()

    assert len(repository_b.fetch()) == 4
    assert repository_b.fetch()[-1].datetime == repository_a.fetch()[-1].datetime


def test_composer_backup_can_be_synchronizeed_to_other_repositories():

    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', force_clear=True))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', force_clear=True))
    repository_c = Repository(adapter=MemoryRepositoryAdapter(bucket='C', force_clear=True))
    backup_policy = TimeIntervalBackupPolicy(1000)

    link_to_b = MemoryRepositoryLink(
        source_bucket='A',
        target_bucket='B'
    )
    link_to_c = MemoryRepositoryLink(
        source_bucket='A',
        target_bucket='C'
    )

    creator = MemoryBackupCreator(bucket='A')
    composer = BackupComposer(
        project='myproject',
        volume='db',
        creator=creator,
        backup_policy=backup_policy,
        cleanup_policy=False,
        synchronizeers=[
            link_to_b,
            link_to_c
        ]
    )

    backups_on_a = repository_a.fetch()
    backups_on_b = repository_b.fetch()
    backups_on_c = repository_c.fetch()
    assert len(backups_on_a) == 0
    assert len(backups_on_b) == 0
    assert len(backups_on_c) == 0

    composer.run()

    backups_on_a = repository_a.fetch()
    backups_on_b = repository_b.fetch()
    backups_on_c = repository_c.fetch()
    assert len(backups_on_a) == 1
    assert len(backups_on_c) == 1


def test_backup_creator_can_be_chained():

    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', force_clear=True))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', force_clear=True))
    repository_c = Repository(adapter=MemoryRepositoryAdapter(bucket='C', force_clear=True))
    repository_d = Repository(adapter=MemoryRepositoryAdapter(bucket='D', force_clear=True))

    creator = MemoryBackupCreator(target_bucket='A')
    link_from_A_to_D = RepositoryLink.chain(
        MemoryRepositoryLink(source_bucket='A', target_bucket='B'),
        MemoryRepositoryLink(source_bucket='B', target_bucket='C'),
        MemoryRepositoryLink(source_bucket='C', target_bucket='D')
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='db',
        datetime='20200420_130000'
    ))
    link_from_A_to_D.synchronize()

    # Check A
    backups = repository_a.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Check B
    backups = repository_b.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Check C
    backups = repository_c.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Check D
    backups = repository_d.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'


def test_backup_creator_chain_can_have_temps_nodes():

    repository_a = Repository(adapter=MemoryRepositoryAdapter(bucket='A', force_clear=True))
    repository_b = Repository(adapter=MemoryRepositoryAdapter(bucket='B', force_clear=True))
    repository_c = Repository(adapter=MemoryRepositoryAdapter(bucket='C', force_clear=True))
    repository_d = Repository(adapter=MemoryRepositoryAdapter(bucket='D', force_clear=True))

    creator = MemoryBackupCreator(
        target_bucket='A',
    )
    link_from_A_to_D = RepositoryLink.chain(
        MemoryRepositoryLink(source_bucket='A', target_bucket='B', sync_policy=MovePolicy()),
        MemoryRepositoryLink(source_bucket='B', target_bucket='C'),
        MemoryRepositoryLink(source_bucket='C', target_bucket='D', sync_policy=MovePolicy())
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='db',
        datetime='20200420_130000'
    ))
    link_from_A_to_D.synchronize()

    # Check A
    backups = repository_a.fetch()
    assert len(backups) == 0

    # Check B
    backups = repository_b.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Check C
    backups = repository_c.fetch()
    assert len(backups) == 0

    # Check D
    backups = repository_d.fetch()
    print()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'


def test_raise_error_if_creator_chain_is_not_compatible():

    with pytest.raises(exp.BuilderChainningIncompatibility):
        MemoryRepositoryLink(
            source=MockMysqlToLocal(directory='/backups')
        )

    with pytest.raises(exp.BuilderChainningIncompatibility):
        RepositoryLink.chain(
            MemoryRepositoryLink(source_bucket='A', target_bucket='B'),
            MockLocalToFtp(directory='/backups', server='ftp.backup')
        )
