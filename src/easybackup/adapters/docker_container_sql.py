import os

from easybackup.core.backup_creator import BackupCreator
from .local import LocalRepositoryAdapter, LocalBackupCreator


class DockerContainerSql(BackupCreator):

    type_tag = 'docker_container_sql'

    def setup(
        self,
        container_name,
        container_user,
        database,
        dump_cmd,
        backup_directory
    ):
        self.container_name = container_name
        self.container_user = container_user
        self.database = database
        self.dump_cmd = dump_cmd
        self.backup_directory = backup_directory

    def target_adapter(self):
        return LocalRepositoryAdapter(directory=self.backup_directory)

    def build_backup(self, backup):
        dump_file_name = self.dump_file_name(backup)

        self.dump(dump_file_name)
        self.tar(dump_file_name, backup)
        self.remove_dump(dump_file_name)

    def dump_file_name(self, backup):
        backup.file_type = 'sql'
        return self.target_adapter().backup_path(backup)

    def dump(self, dump_file_name):
        os.system(self.backup_cmd(dump_file_name))

    def backup_cmd(self, dump_file_name):
        return "docker exec -u {user} {container} {dump_cmd} {database} > \"{dump_file_name}\"".format(**{
            'user': self.container_user,
            'container': self.container_name,
            'dump_cmd': self.dump_cmd,
            'database': self.database,
            'dump_file_name': dump_file_name
        })

    def tar(self, archive_path, backup):
        localBackup = LocalBackupCreator(
            source=archive_path,
            backup_directory=self.backup_directory
        )
        localBackup.build_backup(backup)

    def remove_dump(self, dump_file):
        os.remove(dump_file)
