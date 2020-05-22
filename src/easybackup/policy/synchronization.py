from ..core.repository import Repository
from ..core.clock import Clock
from ..core.volume import Volume
from ..utils.taggable import Taggable


class SynchronizationPolicy(Taggable):

    type_tag = False

    def __init__(self, volume: Volume = False, **conf):
        self.volume = volume
        self.setup(**conf)

    def setup(self, **conf):
        pass

    def to_copy(self, source: Repository, target: Repository):
        """ Determine backups that should be copy from source to target """
        raise NotImplementedError

    def to_delete(self, source: Repository, target: Repository):
        """ Determine backups that should be delete on target """
        raise NotImplementedError


class CopyPastePolicy(SynchronizationPolicy):

    type_tag = 'copypaste'

    """
    Copy all backups present on the source repository
    to the target repository.
    """

    def to_copy(self, source: Repository, target: Repository):
        source_backups = source.fetch(volume=self.volume)
        target_backups = target.fetch(volume=self.volume)

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

    type_tag = 'recent'

    """
    It Guarantee a `minimum` number of backups on the
    target repository. The most recent ones are synchronized.
    """

    def setup(self, minimum: int):
        self.minimum = minimum

    def to_copy(self, source, target):

        source_backups = self.order_backups(source.fetch(volume=self.volume))
        target_backups = target.fetch(volume=self.volume)
        notsync = self.notsync(source_backups, target_backups)

        return notsync[-self.minimum:]

    def to_delete(self, source, target):
        target_backups = self.order_backups(target.fetch(volume=self.volume))
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
