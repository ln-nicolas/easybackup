from . import exceptions as exp
from .backup import Backup
from .repository import Repository
from ..policy.synchronization import SynchronizationPolicy, CopyPastePolicy
from .backup_creator import BackupCreator


class RepositoryLink():

    @classmethod
    def chain(cls, *links):

        if len(links) == 1:
            return links[0]

        if len(links) == 2:
            cls.check_chainning_compatibility(
                links[0], links[1]
            )
            links[0]._chain_to = links[1]
            return links[0]

        else:
            links[0]._chain_to = cls.chain(*links[1:])
            return links[0]

    @classmethod
    def check_chainning_compatibility(cls, link_from, link_to) -> bool:
        """ Check if two creators are compatible to be chaining """

        from_target = link_from.target_adapter()
        source_to = link_to.source_adapter()

        if not source_to:
            raise exp.BuilderChainningIncompatibility

        if type(source_to) is not type(from_target):
            raise exp.BuilderChainningIncompatibility

    def __init__(
        self,
        sync_policy: SynchronizationPolicy = False,
        **conf
    ):
        self.setup(**conf)
        self._chain_to = False
        self._sync_policy = sync_policy or CopyPastePolicy()

        if source:
            self.check_chainning_compatibility(source, self)
            self._source = source

    def setup(self, **conf):
        """ setup creator with custom configuration values """
        raise NotImplementedError

    def source_adapter(self) -> Repository:
        """ Return the source repository """
        raise NotImplementedError

    def target_adapter(self) -> Repository:
        """ Return the repository where backups are stored """
        raise NotImplementedError

    def copy_backup(self, backup: Backup):
        """ Synchronize backup from source to target """
        raise NotImplementedError

    @property
    def target_repository(self) -> Repository:
        return Repository(adapter=self.target_adapter())

    @property
    def source_repository(self) -> Repository:
        return Repository(adapter=self.source_adapter())

    def synchronize(self):
        """ Synchronize all backup from source to target """

        tocopy = self._sync_policy.to_copy(
            source=self.source_repository,
            target=self.target_repository
        )
        todelete = self._sync_policy.to_delete(
            source=self.source_repository,
            target=self.target_repository
        )

        self.copy_backups(tocopy)
        self.delete_backups(todelete)
        if self._chain_to:
            self._chain_to.synchronize()

    def delete_backups(self, backups):
        self.source_repository.cleanup_backups(backups)

    def copy_backups(self, backups):

        if len(backups) == 0:
            return

        self.copy_backup(backups[0])
        self.copy_backups(backups[1:])
