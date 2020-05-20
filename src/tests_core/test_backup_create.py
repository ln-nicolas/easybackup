import pytest
from parameterized import parameterized

from easybackup.core.backup import Backup
from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.core.clock import Clock
from easybackup.core.repository import Repository
from easybackup.policy.backup import TimeIntervalBackupPolicy

from .mock import MemoryBackupCreator, MemoryRepositoryAdapter, clock

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
def test_build_backup(memory_adapter):

    rep = Repository(
        adapter=memory_adapter
    )
    backup_supervisor = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=memory_backup_creator,
        repository=rep
    )
    backup_supervisor.build_backup()

    backups = rep.fetch()

    assert type(backups[-1]) is Backup
    assert backups[-1].project == 'myproject'
    assert backups[-1].volume == 'db'
    assert backups[-1].datetime == '20200422_130000'


def test_compute_if_should_backup_with_a_time_interval_policy_and_no_backups():
    memory_adapter = MemoryRepositoryAdapter(backups=[])
    backups = Repository(adapter=memory_adapter).fetch()
    policy = TimeIntervalBackupPolicy(
        interval=24*60*60
    )
    should = policy.should_backup(backups)
    assert should is True


@parameterized.expand([
    ('20200422_130000', 24*60*60, False),
    ('20200422_140000', 24*60*60, False),
    ('20200423_130000', 24*60*60, True),
    ('20200423_120000', 24*60*60, False),

    ('20200422_130000', 12*60*60, False),
    ('20200422_140000', 12*60*60, False),
    ('20200423_010000', 12*60*60, True),
    ('20200423_130000', 12*60*60, True),
    ('20200423_120000', 12*60*60, True),
])
def test_compute_if_should_backup_with_a_time_interval_policy(clock, interval, expected):
    memory_adapter = MemoryRepositoryAdapter(backups=mockbackups)
    backups = Repository(adapter=memory_adapter).fetch()

    policy = TimeIntervalBackupPolicy(
        interval=interval
    )
    Clock.monkey_now(clock)
    should = policy.should_backup(backups)
    Clock.monkey_now(False)

    assert should == expected


@parameterized.expand([
    ('20200422_130000', 24*60*60, False),
    ('20200422_140000', 24*60*60, False),
    ('20200423_130000', 24*60*60, True),
    ('20200423_120000', 24*60*60, False),

    ('20200422_130000', 12*60*60, False),
    ('20200422_140000', 12*60*60, False),
    ('20200423_130000', 12*60*60, True),
    ('20200423_120000', 12*60*60, False),
])
def test_should_create_new_backup_with_time_interval_backup_policy(clock, backup_interval, should):
    Clock.monkey_now(clock)
    memory_adapter = MemoryRepositoryAdapter(backups=mockbackups)

    rep = Repository(
        adapter=memory_adapter
    )
    last = rep.last_backup()

    backup_policy = TimeIntervalBackupPolicy(backup_interval)

    composer = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=memory_backup_creator,
        repository=rep,
        cleanup_policy=False,
        backup_policy=backup_policy
    )

    composer.run()

    backups = rep.fetch()
    assert backups[-1].datetime == clock if should else last.datetime
    Clock.monkey_now(False)
