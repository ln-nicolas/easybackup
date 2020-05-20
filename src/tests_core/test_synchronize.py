# -*- coding: utf-8 -*-

import pytest
from parameterized import parameterized

from easybackup.core import exceptions as exp
from easybackup.core.backup import Backup
from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.core.repository_link import RepositoryLink, Synchroniser
from easybackup.core.repository import Repository
from easybackup.policy.backup import TimeIntervalBackupPolicy
from easybackup.policy.synchronization import CopyPastePolicy, SynchronizeRecentPolicy
from easybackup.policy.cleanup import LifetimeCleanupPolicy
from easybackup.core.clock import Clock

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

infinity = 1e100


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


def test_copy_backups_with_copypaste_policy():

    adapterA = MemoryRepositoryAdapter(bucket='A', backups=mockbackups)
    adapterB = MemoryRepositoryAdapter(bucket='B', backups=mockbackups[:2])

    repository_a = Repository(adapter=adapterA)
    repository_b = Repository(adapter=adapterB)

    link = MemoryRepositoryLink(adapterA, adapterB)

    assert len(repository_b.fetch()) == 2

    link.synchronize(policy=CopyPastePolicy())

    assert len(repository_b.fetch()) == 4
    assert repository_b.fetch()[-1].datetime == repository_a.fetch()[-1].datetime


def test_composer_backup_can_be_synchronizeed_to_other_repositories():

    adapterA = MemoryRepositoryAdapter(bucket='A', force_clear=True)
    adapterB = MemoryRepositoryAdapter(bucket='B', force_clear=True)
    adapterC = MemoryRepositoryAdapter(bucket='C', force_clear=True)

    repository_a = Repository(adapter=adapterA)
    repository_b = Repository(adapter=adapterB)
    repository_c = Repository(adapter=adapterC)
    backup_policy = TimeIntervalBackupPolicy(1000)

    link_to_b = MemoryRepositoryLink(adapterA, adapterB)
    link_to_c = MemoryRepositoryLink(adapterA, adapterC)

    creator = MemoryBackupCreator(bucket='A')
    composer = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=creator,
        backup_policy=backup_policy,
        cleanup_policy=False,
        synchronizers=[
            Synchroniser(link_to_b, CopyPastePolicy()),
            Synchroniser(link_to_c, CopyPastePolicy())
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


def test_synchronize_recent_policy_cleanup_on_target():

    """
    It Guarantee a `minimum` number of backups on the
    target repository.
    If `age_mini` is greater than 0, all outdated backups
    are deleted if the number of backups is enought.
    """

    mockbackups = [
        'easybackup-myproject-db-20200420_130000.tar',
        'easybackup-myproject-db-20200420_130100.tar',
        'easybackup-myproject-db-20200421_130000.tar',
        'easybackup-myproject-db-20200422_130000.tar',
    ]
    A = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    B = Repository(adapter=MemoryRepositoryAdapter(bucket='B', force_clear=True))

    # copy the most recent from A to B
    policy = SynchronizeRecentPolicy(minimum=1)
    tocopy = policy.to_copy(A, B)
    todelete = policy.to_delete(A, B)
    assert len(todelete) == 0
    assert len(tocopy) == 1

    # copy all from A to B, and delete all on B
    Clock.monkey_now('20200422_130000')
    A = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=[mockbackups[2], mockbackups[3]]))
    B = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=[mockbackups[0], mockbackups[1]]))
    policy = SynchronizeRecentPolicy(minimum=2)
    tocopy = policy.to_copy(A, B)
    todelete = policy.to_delete(A, B)
    assert len(todelete) == 2
    assert len(tocopy) == 2

    # nothing to sync, nothing to delete
    Clock.monkey_now('20200422_130000')
    A = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    B = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=mockbackups))
    policy = SynchronizeRecentPolicy(minimum=5)
    tocopy = policy.to_copy(A, B)
    todelete = policy.to_delete(A, B)
    assert len(todelete) == 0
    assert len(tocopy) == 0

    # nothing to copy, delete the oldest on B
    Clock.monkey_now('20200422_130000')
    A = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    B = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=mockbackups))
    policy = SynchronizeRecentPolicy(minimum=3)
    tocopy = policy.to_copy(A, B)
    todelete = policy.to_delete(A, B)
    assert len(todelete) == 1
    assert len(tocopy) == 0

    # copy the two last ones from A to B, and delete two oldest on B
    mockbackups = [
        'easybackup-myproject-db-20200420_130000.tar',
        'easybackup-myproject-db-20200420_130100.tar',
        'easybackup-myproject-db-20200421_130000.tar',
        'easybackup-myproject-db-20200422_130000.tar',
        'easybackup-myproject-db-20200422_140000.tar',
        'easybackup-myproject-db-20200422_150000.tar',
    ]
    Clock.monkey_now('20200423_130000')
    A = Repository(adapter=MemoryRepositoryAdapter(bucket='A', backups=mockbackups))
    B = Repository(adapter=MemoryRepositoryAdapter(bucket='B', backups=mockbackups[:3]))
    policy = SynchronizeRecentPolicy(minimum=2)
    tocopy = policy.to_copy(A, B)
    todelete = policy.to_delete(A, B)
    assert len(todelete) == 3
    assert len(tocopy) == 2


def test_setup_a_cleanup_policy_on_synchronized_repository():

    adapterA = MemoryRepositoryAdapter(bucket='A', force_clear=True)
    adapterB = MemoryRepositoryAdapter(bucket='B', force_clear=True)

    link = MemoryRepositoryLink(adapterA, adapterB)
    creator = MemoryBackupCreator(bucket='A')

    composer = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=creator,
        backup_policy=TimeIntervalBackupPolicy(1000),
        cleanup_policy=False,
        synchronizers=[
            Synchroniser(
                link,
                SynchronizeRecentPolicy(minimum=2)
            ),
        ]
    )

    Clock.monkey_now('20200101_000000')
    composer.run()

    Clock.monkey_now('20200102_000000')
    composer.run()

    assert len(adapterB.fetch_backups()) == 2

    Clock.monkey_now('20200112_000000')
    composer.run()
    assert len(adapterB.fetch_backups()) == 2
