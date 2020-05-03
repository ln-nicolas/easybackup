from . import exceptions as exp
from typing import List


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
    def parse(cls, name: str) -> dict:

        try:
            _, project, volume, date_and_ext = name.split('-')
            date, ext = date_and_ext.split('.')
        except Exception:
            raise exp.BackupParseNameError

        return {
            'volume': volume,
            'project': project,
            'datetime': date,
            'file_type': ext,
        }

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

    @classmethod
    def fromStr(cls, name: str):
        kwargs = cls.parse(name)
        return Backup(**kwargs)

    def __str__(self):
        return self.format_name(
            volume=self.volume,
            project=self.project,
            datetime=self.datetime,
            file_type=self.file_type,
        )


class Volume():

    def __init__(self, name, project):
        self.name = name
        self.project = project

    def match(self, backups: List[Backup]) -> List[Backup]:
        """ return backup matching volume """
        return list(filter(
            lambda b: b.volume == self.name and b.project == self.project,
            backups
        ))
