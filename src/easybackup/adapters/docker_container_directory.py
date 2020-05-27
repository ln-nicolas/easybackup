import os

from easybackup.core.backup_creator import BackupCreator
from .local import LocalRepositoryAdapter, LocalBackupCreator


class DockerContainerDirectory(BackupCreator):

    type_tag = 'docker_container_directory'

    def setup(
        self,
        container_name,
        container_user,
        directory,
        backup_directory
    ):
        self.container_name = container_name
        self.container_user = container_user
        self.directory = directory
        self.backup_directory = backup_directory

    def target_adapter(self):
        return LocalRepositoryAdapter(directory=self.backup_directory)

    def build_backup(self, backup):
        backup.file_type = 'tar'

        tmp_container_tar = self.tmp_container_tar(backup)
        archive_path = self.target_adapter().backup_path(backup)

        self.tar(tmp_container_tar)
        self.copy(tmp_container_tar, archive_path)
        self.delete_tmp(tmp_container_tar)

    def tar(self, tmp_container_tar):

        cmd = 'docker exec -u {user} {container} tar -czf {tar_file} {directory}'.format(
            user=self.container_user,
            container=self.container_name,
            tar_file=tmp_container_tar,
            directory=self.directory
        )
        os.system(cmd)

    def copy(self, tmp_container_tar, archive_path):

        cmd = 'docker cp {container}:/{tar_file} {archive_path}'.format(
            container=self.container_name,
            tar_file=tmp_container_tar,
            archive_path=archive_path
        )
        os.system(cmd)

    def delete_tmp(self, tmp_container_tar):

        cmd = 'docker exec -u {user} {container} rm {tar_file}'.format(
            user=self.container_user,
            container=self.container_name,
            tar_file=tmp_container_tar
        )
        os.system(cmd)

    def tmp_container_tar(self, backup):
        # copy = backup.copy()
        # copy.file_type = 'tar'
        return backup.formated_name+'.tar'
