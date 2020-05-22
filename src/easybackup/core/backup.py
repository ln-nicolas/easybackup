
class Backup():

    prefix = 'easybackup'

    def __init__(
        self,
        datetime,
        project,
        volume,
        file_type=None
    ):

        self._datetime = datetime
        self._project = project
        self._volume = volume
        self._file_type = file_type

    def copy(self):
        return Backup(
            datetime=self._datetime,
            project=self._project,
            volume=self._volume,
            file_type=self._file_type
        )

    @property
    def datetime(self) -> str:
        """ datetime of backup """
        return self._datetime

    @property
    def project(self) -> str:
        """ backup's project """
        return self._project

    @property
    def volume(self) -> str:
        """ volume's project backup"""
        return self._volume

    @property
    def file_type(self):
        """ backup file type """
        return self._file_type

    @file_type.setter
    def file_type(self, value):
        self._file_type = value

    @property
    def formated_name(self):
        return self.format_name(
            volume=self.volume,
            project=self.project,
            datetime=self.datetime,
            file_type=self.file_type,
        )

    @classmethod
    def format_name(
        cls,
        volume,
        project,
        datetime,
        file_type,
    ):

        str_format = "{prefix}-{project}-{volume}-{datetime}.{file_type}"
        return str_format.format(
            prefix=cls.prefix,
            volume=volume,
            project=project,
            datetime=datetime,
            file_type=file_type,
        )


