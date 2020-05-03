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

    def setup(self, ftp_conf):
        self._host = ftp_conf.get('host')
        self._user = ftp_conf.get('user')
        self._password = ftp_conf.get('password')
        self._directory = ftp_conf.get('directory')

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

    def setup(self, source_directory=False, ftp_conf={}):
        self.source_directory = os.path.abspath(source_directory)
        self.ftp_conf = ftp_conf

    def source_adapter(self):
        return LocalRepositoryAdapter(directory=self.source_directory)

    def target_adapter(self):
        return FtpRepositoryAdapter(ftp_conf=self.ftp_conf)

    def copy_backup(self, backup):

        source_archive = self.source_adapter().backup_path(backup)
        source_archive = open(source_archive, 'rb')

        with self.target_adapter().ftp() as ftp:
            target_archive = self.target_adapter().backup_path(backup)
            ftp.storbinary('STOR '+target_archive, source_archive)


class FtpToLocal(RepositoryLink):

    def setup(self, target_directory=False, ftp_conf={}):
        self.target_directory = os.path.abspath(target_directory)
        self.ftp_conf = ftp_conf

    def source_adapter(self):
        return FtpRepositoryAdapter(ftp_conf=self.ftp_conf)

    def target_adapter(self):
        return LocalRepositoryAdapter(directory=self.target_directory)

    def copy_backup(self, backup):

        source_archive = self.source_adapter().backup_path(backup)
        target_archive = self.target_adapter().backup_path(backup)
        target_archive = open(target_archive, 'wb')

        with self.source_adapter().ftp() as ftp:
            ftp.retrbinary('RETR '+source_archive, target_archive.write)
