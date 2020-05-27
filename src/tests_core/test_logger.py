import pytest

from easybackup.core.repository import Repository
from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.logger import Logger, TextualLogger
from .mock import MemoryBackupCreator, MemoryRepositoryAdapter, clock
from easybackup.i18n import i18n

mockbackups = [
    'easybackup-myproject-db-20200420_130000.tar',
    'easybackup-myproject-db-20200420_130100.tar',
    'easybackup-myproject-db-20200421_130000.tar',
    'easybackup-myproject-db-20200422_130000.tar',
]
memory_backup_creator = MemoryBackupCreator()

INFO_LVL = 20


@pytest.fixture
def memory_adapter():
    return MemoryRepositoryAdapter(backups=mockbackups)


class MockLogAdapter(TextualLogger):

    logs = []

    def log(self, level, message, *args, **kwargs):
        self.logs.append((level, message))


@clock('20200422_130000')
def test_info_log_on_build_backup(memory_adapter):

    rep = Repository(
        adapter=memory_adapter
    )
    backup_supervisor = BackupSupervisor(
        project='myproject',
        volume='db',
        creator=memory_backup_creator,
        repository=rep
    )

    logger = MockLogAdapter()
    Logger.add_logger(logger)

    backup_supervisor.build_backup()
    backup_name = rep.fetch()[-1].formated_name

    assert logger.logs[0][0] == INFO_LVL
    assert logger.logs[0][1] == i18n.t('creating_backup', backup=backup_name, creator=str(memory_backup_creator), repository=str(rep))

    assert logger.logs[1][0] == INFO_LVL
    assert logger.logs[1][1] == i18n.t('backup_has_been_create', backup=backup_name, creator=str(memory_backup_creator), repository=str(rep))
