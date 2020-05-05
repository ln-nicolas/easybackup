
from .backup import Backup, Volume
from .repository import Repository, RepositoryAdapter
from ..policy.synchronization import SynchronizationPolicy, CopyPastePolicy
from ..policy.cleanup import CleanupPolicy
from . import exceptions as exp


class RepositoryLink():

    type_tag_source = False
    type_tag_target = False

    def __init__(
        self,
        source: RepositoryAdapter,
        target: RepositoryAdapter
    ):
        self._source = source
        self._target = target

    def copy_backup(self, backup: Backup):
        """ Synchronize backup from source to target """
        raise NotImplementedError

    @property
    def target_adapter(self) -> RepositoryAdapter:
        return self._target

    @property
    def source_adapter(self) -> RepositoryAdapter:
        return self._source

    @property
    def target_repository(self) -> Repository:
        return Repository(adapter=self._target)

    @property
    def source_repository(self) -> Repository:
        return Repository(adapter=self._source)

    def synchronize(self, policy: SynchronizationPolicy = False):
        """ Synchronize all backup from source to target """

        policy = policy or CopyPastePolicy()

        tocopy = policy.to_copy(
            source=self.source_repository,
            target=self.target_repository
        )
        todelete = policy.to_delete(
            source=self.source_repository,
            target=self.target_repository
        )

        self.copy_backups(tocopy)
        self.target_repository.cleanup_backups(todelete)

    def copy_backups(self, backups):
        list(map(self.copy_backup, backups))

    @classmethod
    def get_source_target_compatible(cls, source, target):

        for sub in cls.__subclasses__():
            if sub.type_tag_source == source and sub.type_tag_target == target:
                return sub

        raise exp.RepositoryLinkNotFound()


class Synchroniser():

    def __init__(
        self,
        link: RepositoryLink,
        sync_policy: SynchronizationPolicy = False,
    ):
        self.link = link
        self.sync_policy = sync_policy or CopyPastePolicy()

    def synchronize(self):
        self.link.synchronize(policy=self.sync_policy)
