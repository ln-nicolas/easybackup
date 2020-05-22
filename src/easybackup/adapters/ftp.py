import os
from contextlib import contextmanager
from ftplib import FTP
from pathlib import Path
from typing import List
import tarfile


from easybackup.core.backup import Backup
from easybackup.core.repository_link import RepositoryLink
from easybackup.core.lexique import ARCHIVE_TYPE
from easybackup.core.repository import RepositoryAdapter

from .local import LocalRepositoryAdapter


class FtpRepositoryAdapter(RepositoryAdapter):

    type_tag = 'ftp'

    def setup(self, host, user, password, directory):
        self._host = host
        self._user = user
        self._password = password
        self._directory = directory

    @contextmanager
    def ftp(self):
        try:
            ftp = FTP(
                host=self._host,
                user=self._user,
                passwd=self._password
            )
            ftp.cwd(self._directory)
            yield ftp
        finally:
            if ftp:
                ftp.close()

    def fetch_backups(self):
        return list(map(self.filename_to_backup, self.list_directory_filenames()))

    def cleanup_backups(self, backups):
        filenames = map(self.backup_to_filename, backups)
        paths = map(self.path, filenames)

        with self.ftp() as ftp:
            list(map(ftp.delete, paths))

    def list_directory_filenames(self):
        with self.ftp() as ftp:
            files = ftp.nlst()
        return filter(self.filename_match_backup, files)

    def backup_path(self, backup):
        archive_name = self.backup_to_filename(backup)
        archive_path = self.path(archive_name)
        return archive_path

    def path(self, filename):
        return os.path.join('{dir}/{filename}'.format(
            dir=self._directory,
            filename=filename
        ))


class LocalToFtp(RepositoryLink):

    type_tag_source = 'local'
    type_tag_target = 'ftp'

    def copy_backup(self, backup):

        source_archive = self.source_adapter.backup_path(backup)
        source_archive = open(source_archive, 'rb')

        with self.target_adapter.ftp() as ftp:
            target_archive = self.target_adapter.backup_path(backup)
            ftp.storbinary('STOR '+target_archive, source_archive)


class FtpToLocal(RepositoryLink):

    type_tag_source = 'ftp'
    type_tag_target = 'local'

    def copy_backup(self, backup):

        source_archive = self.source_adapter.backup_path(backup)
        target_archive = self.target_adapter.backup_path(backup)
        target_archive = open(target_archive, 'wb')

        with self.source_adapter.ftp() as ftp:
            ftp.retrbinary('RETR '+source_archive, target_archive.write)
