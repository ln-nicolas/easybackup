
from .backup import Backup
from .repository import Repository
from ..utils.taggable import Taggable
from .hook import Hook


class BackupCreator(Taggable):

    type_tag = False

    def __init__(self, **conf):
        self.setup(**conf)

    def setup(self, **conf):
        """ setup creator with custom configuration values """
        raise NotImplementedError

    def target_adapter(self) -> Repository:
        """ Return the repository where backups are stored """
        raise NotImplementedError

    def do_build_backup(self, backup: Backup) -> Backup:
        Hook.plays('before_build_backup', creator=self, backup=backup)
        self.build_backup(backup)
        Hook.plays('after_build_backup', creator=self, backup=backup)

    def build_backup(self, backup: Backup) -> Backup:
        """ build backup """
        raise NotImplementedError

    @property
    def target_repository(self) -> Repository:
        """ Repository where backups are store. """
        return Repository(adapter=self.target_adapter())

    @classmethod
    def by_type_tag(cls, tag):
        for sub in cls.__subclasses__():
            if sub.type_tag == tag:
                return sub
