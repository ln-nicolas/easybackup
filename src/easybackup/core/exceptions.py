from ..i18n import i18n


class EasyBackupException(Exception):

    def __init__(self, code, **kwargs):
        self.code = code
        self.message = i18n.t(code, **kwargs)
        self.kwargs = kwargs

    def __str__(self):
        return self.message


class BackupParseNameError(EasyBackupException):
    pass


class RepositoryLinkNotFound(EasyBackupException):
    pass


class BuilderChainningIncompatibility(EasyBackupException):
    pass
