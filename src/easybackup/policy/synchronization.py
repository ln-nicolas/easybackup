from ..core.repository import Repository


class SynchronizationPolicy():

    def to_copy(self, source: Repository, target: Repository):
        """ Determine backups that should be copy from source to target """
        raise NotImplementedError

    def to_delete(self, source: Repository, target: Repository):
        """ Determine backups that should be delete in target """
        raise NotImplementedError


class CopyPastePolicy(SynchronizationPolicy):

    def to_copy(self, source: Repository, target: Repository):
        source_backups = source.fetch()
        target_backups = target.fetch()

        return list(filter(
            lambda backup: str(backup) not in [str(b) for b in target_backups],
            source_backups
        ))

    def to_delete(self, source: Repository, target: Repository):
        return []


class MovePolicy(CopyPastePolicy):

    def to_delete(self, source: Repository, target: Repository):
        return source.fetch()
