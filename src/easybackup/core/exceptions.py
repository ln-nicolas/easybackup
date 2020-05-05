
class EasyBackupException(Exception):
    pass


class BackupParseNameError(EasyBackupException):
    pass


class RepositoryLinkNotFound(EasyBackupException):
    pass


class BuilderChainningIncompatibility(EasyBackupException):
    pass
