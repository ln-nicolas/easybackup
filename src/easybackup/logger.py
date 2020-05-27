from easybackup.core.hook import Hook
from easybackup.core.lexique import human_dt
from .i18n import i18n


class Logger():

    loggers = []

    # see level here https://docs.python.org/3/library/logging.html#logging-levels
    map_lvl = {
        'CRITICAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0
    }

    @classmethod
    def add_logger(cls, logger):
        cls.loggers.append(logger)

    @classmethod
    def log_event(cls, lvl, event, *args, **kwargs):
        for logger in cls.loggers:

            if hasattr(logger, event+'_message'):
                message = getattr(logger, event+'_message')(*args, **kwargs)
            else:
                message = getattr(TextualLogger(), event+'_message')(*args, **kwargs)

            if type(lvl) is str:
                lvl = cls.map_lvl.get(lvl)

            logger.log(lvl, message)

    def log(self, lvl, message):
        raise NotImplementedError


class TextualLogger(Logger):

    def before_build_backup_message(self, creator, backup, repository):
        return i18n.t('creating_backup', backup=backup.formated_name, creator=creator, repository=str(repository))

    def after_build_backup_message(self, creator, backup, repository):
        return i18n.t('backup_has_been_create', backup=backup.formated_name, creator=creator, repository=str(repository))

    def before_fetch_backups_message(self, repository, volume):
        return i18n.t('fetching_backups_on_repository', repository=str(repository), volume=str(volume))

    def after_fetch_backups_message(self, repository, volume, backups):

        if len(backups) == 0:
            return i18n.t('repository_fetch_backups_empty_results', repository=str(repository), volume=str(volume))

        lastone = sorted(backups, key=lambda b: b.datetime)[-1]

        return i18n.t(
            'repository_fetch_backups_results',
            repository=str(repository),
            count=len(backups),
            last_datetime=human_dt(lastone.datetime),
            volume=str(volume)
        )

    def on_synchronize_repository_message(self, volume, source, target, policy, tocopy, todelete):

        return i18n.t(
            'on_synchronize_repositories',
            volume=str(volume),
            source=str(source),
            target=str(target),
            policy=str(policy),
            count_tocopy=len(tocopy),
            count_todelete=len(todelete)
        )

    def on_checking_volume_backup_policy_message(self, volume, policy, backups, it_should):

        if it_should:
            return i18n.t(
                'should_backup_volume_according_to_backup_policy',
                volume=str(volume),
                policy=str(policy)
            )
        else:
            return i18n.t(
                'skip_backup_volume_according_to_backup_policy',
                volume=str(volume),
                policy=str(policy)
            )

    def on_cleanup_backups_message(self, volume, tocleanup, policy):

        return i18n.t(
            'on_cleanup_backup_with_policy',
            volume=volume,
            count_tocleanup=len(tocleanup),
            policy=policy
        )


@Hook.register('before_build_backup')
def hook_before_build_backup(*args, **kwargs):
    Logger.log_event('INFO', 'before_build_backup', *args, **kwargs)


@Hook.register('after_build_backup')
def hook_after_build_backup(*args, **kwargs):
    Logger.log_event('INFO', 'after_build_backup', *args, **kwargs)


@Hook.register('before_fetch_backups')
def hook_before_fetch_backups(*args, **kwargs):
    Logger.log_event('INFO', 'before_fetch_backups', *args, **kwargs)


@Hook.register('after_fetch_backups')
def hook_after_fetch_backups(*args, **kwargs):
    Logger.log_event('INFO', 'after_fetch_backups', *args, **kwargs)


@Hook.register('on_synchronize_repository')
def hook_on_synchronize_repository(*args, **kwargs):
    Logger.log_event('INFO', 'on_synchronize_repository', *args, **kwargs)


@Hook.register('on_checking_volume_backup_policy')
def hook_on_checking_volume_backup_policy(*args, **kwargs):
    Logger.log_event('INFO', 'on_checking_volume_backup_policy', *args, **kwargs)


@Hook.register('on_cleanup_backups')
def hook_on_cleanup_backups(*args, **kwargs):
    Logger.log_event('INFO', 'on_cleanup_backups', *args, **kwargs)
