from easybackup.core.hook import Hook
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
        for logger in Logger.loggers:

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

    def before_build_backup_message(self, creator, backup):
        return i18n.t('creating_backup', backup=backup.formated_name, creator=creator)

    def after_build_backup_message(self, creator, backup):
        return i18n.t('backup_has_been_create', backup=backup.formated_name, creator=creator)


@Hook.register('before_build_backup')
def hook_before_build_backup(*args, **kwargs):
    Logger.log_event('INFO', 'before_build_backup', *args, **kwargs)


@Hook.register('after_build_backup')
def hook_after_build_backup(*args, **kwargs):
    Logger.log_event('INFO', 'after_build_backup', *args, **kwargs)
