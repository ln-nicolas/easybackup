import subprocess


class UnixPlatform():

    @classmethod
    def touch(cls, path):
        subprocess.run(['touch', path])