import pytest
import os
import subprocess


@pytest.fixture(scope="function")
def temp_directory():

    directory = "./tmp_easybackup_tests_local"
    if os.path.exists(directory):
        subprocess.run(['rm', '-rf', directory])

    os.mkdir(directory)
    os.mkdir(directory+'/backups-twine')
    os.mkdir(directory+'/backups')
    os.mkdir(directory+'/restore')

    def func(path=""):
        return (directory+'/'+path)

    yield func

    #subprocess.run(['rm', '-rf', directory])
