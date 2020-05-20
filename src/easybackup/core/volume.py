from typing import List
from .backup import Backup


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
