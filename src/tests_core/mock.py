import functools
from typing import List

from easybackup.core import exceptions as exp
from easybackup.core.backup import Backup
from easybackup.core.backup_creator import BackupCreator
from easybackup.core.repository_link import RepositoryLink
from easybackup.core.clock import Clock
from easybackup.core.repository import RepositoryAdapter

BUCKET_A = []
BUCKET_B = []
BUCKET_C = []
BUCKET_D = []


def clock(datetime):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            Clock.monkey_now(datetime)
            func(*args, **kwargs)
            Clock.monkey_now(False)
        return wrapper
    return decorator


class MemoryRepositoryAdapter(RepositoryAdapter):

    def setup(self, backups=[], bucket='A', force_clear=False):
        self.bucket = bucket
        if backups:
            self.backups = backups
        if force_clear:
            self.backups = []

    def fetch_backups(self) -> List[Backup]:
        return [
            Backup(**self.parse(name)) for name in self.backups
        ]

    def cleanup_backups(self, backups: List[Backup]):
        self.backups = [
            backup for backup in self.backups
            if backup not in [self.backup_to_filename(b) for b in backups]
        ]

    @classmethod
    def parse(cls, name: str) -> dict:

        try:
            _, project, volume, date_and_ext = name.split('-')
            date, ext = date_and_ext.split('.')
        except Exception:
            raise exp.BackupParseNameError

        return {
            'volume': volume,
            'project': project,
            'datetime': date,
            'file_type': ext,
        }

    @property
    def backups(self):
        global BUCKET_A
        global BUCKET_B
        global BUCKET_C
        global BUCKET_D
        return {
            'A': BUCKET_A,
            'B': BUCKET_B,
            'C': BUCKET_C,
            'D': BUCKET_D
        }.get(self.bucket)

    @backups.setter
    def backups(self, values):
        global BUCKET_A
        global BUCKET_B
        global BUCKET_C
        global BUCKET_D
        if self.bucket == 'A':
            BUCKET_A = values
        if self.bucket == 'B':
            BUCKET_B = values
        if self.bucket == 'C':
            BUCKET_C = values
        if self.bucket == 'D':
            BUCKET_D = values


class MemoryRepositoryLink(RepositoryLink):

    def setup(self, source_bucket=False, target_bucket='A'):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket

    def source_adapter(self):
        return MemoryRepositoryAdapter(bucket=self.source_bucket)

    def target_adapter(self):
        return MemoryRepositoryAdapter(bucket=self.target_bucket)

    def copy_backup(self, backup):
        self.target_backups.append(
            backup.formated_name
        )

    @property
    def source_backups(self):
        global BUCKET_A
        global BUCKET_B
        global BUCKET_C
        global BUCKET_D
        return {
            'A': BUCKET_A,
            'B': BUCKET_B,
            'C': BUCKET_C,
            'D': BUCKET_D
        }.get(self.source_bucket)

    @property
    def target_backups(self):
        global BUCKET_A
        global BUCKET_B
        global BUCKET_C
        global BUCKET_D
        return {
            'A': BUCKET_A,
            'B': BUCKET_B,
            'C': BUCKET_C,
            'D': BUCKET_D
        }.get(self.target_bucket)


class MemoryBackupCreator(BackupCreator):

    RepositoryAdapter = MemoryRepositoryAdapter

    def setup(self, bucket='A', source_bucket=False, target_bucket='A'):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket or bucket

    def source_adapter(self):
        return MemoryRepositoryAdapter()

    def target_adapter(self):
        return MemoryRepositoryAdapter(bucket=self.target_bucket)

    def build_backup(self, backup):
        backup.file_type = 'tar'
        self.target_backups.append(MemoryRepositoryAdapter.backup_to_filename(backup))

    @property
    def source_backups(self):
        global BUCKET_A
        global BUCKET_B
        global BUCKET_C
        global BUCKET_D
        return {
            'A': BUCKET_A,
            'B': BUCKET_B,
            'C': BUCKET_C,
            'D': BUCKET_D
        }.get(self.source_bucket)

    @property
    def target_backups(self):
        global BUCKET_A
        global BUCKET_B
        global BUCKET_C
        global BUCKET_D
        return {
            'A': BUCKET_A,
            'B': BUCKET_B,
            'C': BUCKET_C,
            'D': BUCKET_D
        }.get(self.target_bucket)


class MockLocalRepository(RepositoryAdapter):
    def __init__(self, directory):
        self.directory = directory

    def setup(self, **conf):
        pass

    def fetch_backups(self):
        return []

    def cleanup_backups(self, backups):
        pass


class MockFtpRepository(RepositoryAdapter):
    def __init__(self, server):
        self.server = server

    def setup(self, **conf):
        pass

    def fetch_backups(self):
        return []

    def cleanup_backups(self, backups):
        pass


class MockMysqlToLocal(BackupCreator):

    def setup(self, directory=False):
        self.directory = directory

    def target_adapter(self):
        return MockLocalRepository(
            directory=self.directory
        )

    def build_backup(self, *args):
        pass


class MockLocalToFtp(RepositoryLink):

    def setup(self, directory=False, server=False):
        self.server = server
        self.directory = directory

    def source_adapter(self):
        return MockLocalRepository(
            directory=self.directory
        )

    def target_adapter(self):
        return MockFtpRepository(
            server=self.server
        )

    def build_backup(self, *args):
        pass
