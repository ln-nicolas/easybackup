from .exceptions import EasyBackupException
from ..i18n import i18n


class EasyBackupConfigurationError(EasyBackupException):

    def __init__(self, code, **kwargs):
        self.code = code
        self.message = i18n.t(code, **kwargs)

    def __str__(self):
        return self.message


class ConfigurationProject():

    def __init__(self, name, volumes):
        self.name = name
        self.volumes = volumes


class Configuration():

    def __init__(
        self,
        version,
        projects,
        composers,
        repositories
    ):
        self.version = version
        self.projects = projects
        self.composer = composers
        self.repositories = repositories


class ConfigurationLoader():

    @classmethod
    def load(cls, document: str) -> Configuration:
        """ load configuration from string document """
        raise NotImplementedError
