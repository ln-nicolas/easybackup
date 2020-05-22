
from easybackup.core.backup import Backup
from easybackup.adapters.docker_container_directory import DockerContainerDirectory

from tests_core.mock import clock
import os
from .utils import temp_directory
import tarfile

# source env
DIRECTORY_DOCKER_CONTAINER_NAME = ''
DIRECTORY_DOCKER_CONTAINER_USER = ''
DIRECTORY_DOCKER_CONTAINER_DIRECTORY = ''
exec(open('./.env').read(), globals())


@clock('20200102_120000')
def test_backup_docker_container_sql_database_to_local_repository(temp_directory):

    creator = DockerContainerDirectory(
        container_name=DIRECTORY_DOCKER_CONTAINER_NAME,
        container_user=DIRECTORY_DOCKER_CONTAINER_USER,
        directory=DIRECTORY_DOCKER_CONTAINER_DIRECTORY,
        backup_directory=temp_directory('backups')
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='directory',
        datetime='20200420_130000'
    ))

    adapter = creator.target_adapter()
    backups = creator.target_repository.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Test backup extraction
    tar_file = adapter.backup_to_filename(backups[0])
    tar = tarfile.open(adapter.path(tar_file))
    tar.extractall(temp_directory('backups'))

    # Integrity check
    extract_ls = os.system('ls -sh '+temp_directory('backups')+DIRECTORY_DOCKER_CONTAINER_DIRECTORY)
    docker_ls = os.system('docker exec -u '+DIRECTORY_DOCKER_CONTAINER_USER+' '+DIRECTORY_DOCKER_CONTAINER_NAME+' ls -sh '+DIRECTORY_DOCKER_CONTAINER_DIRECTORY)
    assert extract_ls == docker_ls
