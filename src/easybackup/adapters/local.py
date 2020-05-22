import os
import random
import string
import tarfile
import shutil

from typing import List
from easybackup.core.backup import Backup
from easybackup.core.backup_creator import BackupCreator
from easybackup.core.repository_link import RepositoryLink
from easybackup.core.repository import RepositoryAdapter


class LocalRepositoryAdapter(RepositoryAdapter):

    type_tag = 'local'

    def setup(self, directory):
        self.directory = directory

    def fetch_backups(self) -> List[Backup]:
        backup_filenames = filter(
            self.filename_match_backup,
            self.list_directory_filenames()
        )
        backups = map(
            self.filename_to_backup,
            backup_filenames
        )

        return sorted(backups, key=lambda b: b.datetime)

    def cleanup_backups(self, backups):
        filenames = map(self.backup_to_filename, backups)
        paths = map(self.path, filenames)
        list(map(lambda backup: os.remove(backup), paths))

    def list_directory_filenames(self):
        return list(filter(
            lambda f: os.path.isfile(self.path(f)),
            os.listdir(self.directory)
        ))

    def backup_path(self, backup):
        archive_name = self.backup_to_filename(backup)
        archive_path = self.path(archive_name)
        return archive_path

    def path(self, filename):
        return os.path.join('{dir}/{filename}'.format(
            dir=self.directory,
            filename=filename
        ))


class LocalBackupCreator(BackupCreator):

    type_tag = 'local'
    file_type = 'tar'

    def setup(self, source, backup_directory):
        self.source = source
        self.source_is_file = os.path.isfile(self.source)
        self.source_is_directory = os.path.isdir(self.source)
        self.backup_directory = backup_directory

    def target_adapter(self):
        return LocalRepositoryAdapter(directory=self.backup_directory)

    def build_backup(self, backup):
        backup.file_type = self.file_type
        archive_path = self.target_adapter().backup_path(backup)+'.'+self.file_type
        self._backup_source(archive_path)
        return backup

    def _backup_source(self, archive):

        if self.source_is_file:
            self._backup_source_file(archive)

        if self.source_is_directory:
            self._backup_source_directory(archive)

    def _backup_source_file(self, archive):

        with tarfile.open(archive, 'x:gz') as tar:
            with open(self.source, 'rb') as f:
                info = tar.gettarinfo(self.source, arcname=os.path.basename(self.source))
                tar.addfile(info, f)

    def _backup_source_directory(self, archive):

        with tarfile.open(archive, "x:gz") as tar:
            tar.add(self.source, arcname=os.path.basename(self.source))


class LocalToLocal(RepositoryLink):

    type_tag_source = 'local'
    type_tag_target = 'local'

    def copy_backup(self, backup):
        source_path = self.source_adapter.backup_path(backup)
        target_path = self.target_adapter.backup_path(backup)
        shutil.copyfile(source_path, target_path)
