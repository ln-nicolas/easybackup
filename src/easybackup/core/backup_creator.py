
from . import exceptions as exp
from .backup import Backup
from .repository import Repository
from ..policy.synchronization import SynchronizationPolicy, CopyPastePolicy


class BackupCreator():

    def __init__(self, **conf):
        self.setup(**conf)

    def setup(self, **conf):
        """ setup creator with custom configuration values """
        raise NotImplementedError

    def target_adapter(self) -> Repository:
        """ Return the repository where backups are stored """
        raise NotImplementedError

    def build_backup(self, backup: Backup) -> Backup:
        """ build backup """
        raise NotImplementedError

    @property
    def target_repository(self) -> Repository:
        """ Repository where backups are store. """
        return Repository(adapter=self.target_adapter())