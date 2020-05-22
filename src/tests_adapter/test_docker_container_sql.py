
from easybackup.core.backup import Backup
from easybackup.adapters.docker_container_sql import DockerContainerSql

from tests_core.mock import clock
from .utils import temp_directory
import tarfile

# source env
SQL_DOCKER_CONTAINER_NAME = ''
SQL_DOCKER_CONTAINER_USER = ''
SQL_DOCKER_CONTAINER_DB = ''
SQL_DOCKER_CONTAINER_DUMP_CMD = ''
exec(open('./.env').read(), globals())


@clock('20200102_120000')
def test_backup_docker_container_sql_database_to_local_repository(temp_directory):

    creator = DockerContainerSql(
        container_name=SQL_DOCKER_CONTAINER_NAME,
        container_user=SQL_DOCKER_CONTAINER_USER,
        database=SQL_DOCKER_CONTAINER_DB,
        dump_cmd=SQL_DOCKER_CONTAINER_DUMP_CMD,
        backup_directory=temp_directory('backups')
    )

    creator.build_backup(Backup(
        project='myproject',
        volume='db',
        datetime='20200420_130000'
    ))

    adapter = creator.target_adapter()
    backups = creator.target_repository.fetch()
    assert len(backups) == 1
    assert backups[0].datetime == '20200420_130000'

    # Test backup extraction
    tar_file = adapter.backup_to_filename(backups[0])
    assert tar_file == 'easybackup-myproject-db-20200420_130000.tar'

    backups[0].file_type = 'sql'
    dump_file = adapter.backup_to_filename(backups[0])

    tar = tarfile.open(adapter.path(tar_file))
    tar.extractall(temp_directory('backups'))
    extract_file = open(temp_directory('backups/'+dump_file), "r").read()
    assert len(extract_file) > 0
