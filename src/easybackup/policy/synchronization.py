from ..core.repository import Repository
from ..core.clock import Clock


class SynchronizationPolicy():

    def to_copy(self, source: Repository, target: Repository):
        """ Determine backups that should be copy from source to target """
        raise NotImplementedError

    def to_delete(self, source: Repository, target: Repository):
        """ Determine backups that should be delete on target """
        raise NotImplementedError


class CopyPastePolicy(SynchronizationPolicy):

    """
    Copy all backups present on the source repository
    to the target repository.
    """

    def to_copy(self, source: Repository, target: Repository):
        source_backups = source.fetch()
        target_backups = target.fetch()

        return list(filter(
            lambda backup: backup.formated_name not in [b.formated_name for b in target_backups],
            source_backups
        ))

    def to_delete(self, source: Repository, target: Repository):
        return []


class ClonePolicy(SynchronizationPolicy):

    """
    It Guarantee that target repository is a clone
    of source repository
    """
    pass


class SynchronizeRecentPolicy(SynchronizationPolicy):

    """
    It Guarantee a `minimum` number of backups on the
    target repository. The most recent ones are synchronized.
    """

    def __init__(self, minimum: int):
        self.minimum = minimum

    def to_copy(self, source, target):

        source_backups = self.order_backups(source.fetch())
        target_backups = target.fetch()
        notsync = self.notsync(source_backups, target_backups)

        return notsync[-self.minimum:]

    def to_delete(self, source, target):
        target_backups = self.order_backups(target.fetch())
        target_backups = sorted(
            target_backups,
            key=lambda backup: backup.datetime
        )
        tocopy = self.to_copy(source, target)

        overflow = (len(target_backups) + len(tocopy)) - self.minimum
        if overflow > 0:
            return target_backups[:overflow]
        else:
            return []

    def notsync(self, source_backups, target_backups):
        return list(filter(
            lambda backup: backup.formated_name not in [b.formated_name for b in target_backups],
            source_backups
        ))

    def order_backups(self, backups):
        return sorted(
            backups,
            key=lambda backup: backup.datetime
        )
