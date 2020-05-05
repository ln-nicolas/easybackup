import pytest
from parameterized import parameterized

from easybackup.core.backup import Backup
from easybackup.core.clock import Clock
from easybackup.core.repository import Repository
from easybackup.policy.cleanup import LifetimeCleanupPolicy

from .mock import MemoryRepositoryAdapter, clock

mockbackups = [
    'easybackup-myproject-db-20200420_130000.tar',
    'easybackup-myproject-db-20200420_130100.tar',
    'easybackup-myproject-db-20200421_130000.tar',
    'easybackup-myproject-db-20200422_130000.tar',
]


@pytest.fixture
def memory_adapter():
    return MemoryRepositoryAdapter(backups=mockbackups)


@parameterized.expand([
    ('20200101_100000', '20200102_110000', 24*60*60, True),
    ('20200101_100000', '20200102_090000', 24*60*60, False),
    ('20200101_100000', '20200102_020001', 12*60*60, True),
    ('20200101_100000', '20200102_015959', 24*60*60, False)
])
def test_compare_datetime(date, reference, max_age, assertion):
    Clock.monkey_now(reference)
    policy = LifetimeCleanupPolicy(
        max_age=max_age,
        minimum=0
    )
    outdated = policy.outdated(date)
    assert outdated is assertion
    Clock.monkey_now(False)


@clock('20200102_120000')
def test_filter_backups():

    # Without Backups
    backups = []
    policy = LifetimeCleanupPolicy(
        max_age=1*60*60,
        minimum=0
    )
    tokeep = policy.filter_backups_to_keep(backups)
    tocleanup = policy.filter_backups_to_cleanup(backups)
    assert len(tokeep) == 0
    assert len(tocleanup) == 0

    dates = [
        '20200101_100000',
        '20200101_120000',
        '20200101_140000',
        '20200101_160000'
    ]
    backups = [
        Backup(
            datetime=date,
            project=False,
            volume=False,
            file_type=False
        )
        for date in dates
    ]

    # Keep Zero
    policy = LifetimeCleanupPolicy(
        max_age=1*60*60,
        minimum=0
    )
    tokeep = policy.filter_backups_to_keep(backups)
    tocleanup = policy.filter_backups_to_cleanup(backups)
    assert len(tokeep) == 0
    assert len(tocleanup) == 4

    # Keep One
    policy = LifetimeCleanupPolicy(
        max_age=24*60*60,
        minimum=0
    )
    tokeep = policy.filter_backups_to_keep(backups)
    tocleanup = policy.filter_backups_to_cleanup(backups)
    assert len(tocleanup) == 1
    assert tocleanup[0].datetime == '20200101_100000'
    assert len(tokeep) == 3
    assert tokeep[0].datetime == '20200101_120000'
    assert tokeep[1].datetime == '20200101_140000'
    assert tokeep[2].datetime == '20200101_160000'

    # Keep at least 2
    policy = LifetimeCleanupPolicy(
        max_age=1*60*60,
        minimum=2
    )
    tokeep = policy.filter_backups_to_keep(backups)
    tocleanup = policy.filter_backups_to_cleanup(backups)
    assert len(tokeep) == 2
    assert len(tocleanup) == 2
    assert tocleanup[0].datetime == '20200101_100000'
    assert tocleanup[1].datetime == '20200101_120000'
    assert tokeep[0].datetime == '20200101_140000'
    assert tokeep[1].datetime == '20200101_160000'


@clock('20200422_130000')
def test_compute_backups_to_cleanup(memory_adapter):

    policy = LifetimeCleanupPolicy(
        max_age=24*60*60,
        minimum=2
    )

    rep = Repository(
        adapter=memory_adapter
    )

    tocleanup = rep.tocleanup(policy=policy)

    assert len(tocleanup) == 2
    assert tocleanup[0].datetime == '20200420_130000'


@clock('20200422_130000')
def test_cleanup_backups(memory_adapter):

    policy = LifetimeCleanupPolicy(
        max_age=24*60*60,
        minimum=2
    )

    rep = Repository(
        adapter=memory_adapter,
    )

    rep.cleanup(policy=policy)
    backups = rep.fetch()

    assert len(backups) == 2
    assert backups[0].datetime == '20200421_130000'
    assert backups[1].datetime == '20200422_130000'
