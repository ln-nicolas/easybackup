import pytest

from easybackup.core.hook import Hook
from easybackup.core.repository import Repository
from easybackup.core.backup_supervisor import BackupSupervisor
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


def test_register_hook():

    obj = {'done': False}

    @Hook.register('test_hook')
    def do_something(obj):
        obj['done'] = True

    Hook.plays('test_hook', obj)
    assert obj['done'] is True


@clock('20200422_130000')
def test_emit_event_on_backup_build(memory_adapter):

    rep = Repository(
        adapter=memory_adapter
    )
    backup_supervisor = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=memory_backup_creator,
        repository=rep
    )

    backup_list = []
    @Hook.register('before_build_backup')
    @Hook.register('after_build_backup')
    def put_backup_on_list(creator, backup, repository):
        backup_list.append(backup)

    backup_supervisor.build_backup()
    backups = rep.fetch()
    assert backups[-1].formated_name == backup_list[0].formated_name
    assert backups[-1].formated_name == backup_list[1].formated_name
