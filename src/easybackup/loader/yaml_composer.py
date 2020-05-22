import re

import yaml

from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.core.backup_creator import BackupCreator
from easybackup.core.repository import Repository, RepositoryAdapter
from easybackup.core.repository_link import RepositoryLink, Synchroniser
from easybackup.core.volume import Volume
from easybackup.policy.backup import BackupPolicy
from easybackup.policy.cleanup import CleanupPolicy
from easybackup.policy.synchronization import SynchronizationPolicy
from easybackup.core.lexique import is_number_version
from easybackup.core.exceptions import EasyBackupException
from easybackup.adapters import *


try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class YamlComposerException(EasyBackupException):
    pass


class YamlComposer():

    def __init__(self, document):
        obj = (yaml.load(document, Loader=Loader) or {})

        self.obj = obj
        self._repositories = []
        self._projects = []
        self._composers = []

        self.check_version_number()
        self.check_projects()
        self.check_repositories()

    @property
    def repositories(self):

        if not self._repositories:
            self._repositories = []
            repositories = self.obj.get('repositories', {})
            for name, conf in repositories.items():
                adapter = self.build_object(RepositoryAdapter, conf)
                self._repositories.append(Repository(adapter, name=name))

        return self._repositories

    @property
    def composers(self):
        if not self._composers:
            self._composers = []
            for conf in self.volumes:
                composer = self.build_backup_supervisor(conf)
                self._composers.append(composer)

        return self._composers

    def check_version_number(self):
        version = self.obj.get('version')
        if (not version) or (type(version) is not str) or (not is_number_version(version)):
            raise YamlComposerException('version_number_is_missing_or_invalid')

    def check_projects(self):
        projects = self.obj.get('projects')
        if not projects:
            raise YamlComposerException('configuration_should_have_at_least_one_project')

        if type(projects) is not dict:
            raise YamlComposerException('projects_should_be_a_dictionnary')

    def check_repositories(self):
        repositories = self.obj.get('repositories')
        if repositories and type(repositories) is not dict:
            raise YamlComposerException('repositories_should_be_a_dictionnary')

    def build_backup_supervisor(self, conf):

        backup_policy = self.build_object(BackupPolicy, conf['backup_policy'], type_tag_name='policy')
        creator = self.build_object(BackupCreator, conf['creator'])

        cleanup_policy = False
        if conf['cleanup_policy']:
            cleanup_policy = self.build_object(CleanupPolicy, conf['cleanup_policy'], type_tag_name='policy')

        synchronizers = []
        if conf['synchronizers']:
            for name_repo, sync_conf in conf['synchronizers'].items():

                repository = False
                for rep in self.repositories:
                    if rep.name == name_repo:
                        repository = rep

                policy = False
                if sync_conf.get('policy'):
                    policy = self.build_object(SynchronizationPolicy, sync_conf, type_tag_name='policy')
                    policy.volume = Volume(name=conf.get('volume'), project=conf.get('project'))

                source_adapter = creator.target_adapter()
                target_adapter = repository.adapter

                link_self = RepositoryLink.get_source_target_compatible(
                    source=source_adapter.type_tag,
                    target=target_adapter.type_tag
                )

                link = link_self(source=source_adapter, target=target_adapter)
                synchronizers.append(Synchroniser(link=link, sync_policy=policy))

        return BackupSupervisor(
            project=conf.get('project'),
            volume=conf.get('volume'),
            creator=creator,
            backup_policy=backup_policy,
            cleanup_policy=cleanup_policy,
            synchronizers=synchronizers
        )

    @property
    def volumes(self):
        projects = self.obj.get('projects')
        for project_name, volumes in projects.items():

            if type(volumes) is not dict:
                continue

            for volume_name, volume_conf in volumes.items():

                backup_policy = volume_conf.get('backup_policy')
                if not backup_policy:
                    raise YamlComposerException('backup_policy_is_required', project=project_name, volume=volume_name)

                cleanup_policy = volume_conf.get('cleanup_policy', False)
                dispatchers = volume_conf.get('dispatchers', [])

                volume = {
                    'project': project_name,
                    'volume': volume_name,
                    'creator': volume_conf,
                    'backup_policy': backup_policy,
                    'cleanup_policy': cleanup_policy,
                    'synchronizers': dispatchers
                }
                yield volume

    @classmethod
    def backup_policy(cls, conf):

        policy_conf = conf.get('backup_policy', False)

        if not policy_conf:
            return False

        return cls.build_object(BackupPolicy, conf)

    @classmethod
    def build_object(cls, base_cls, conf, type_tag_name='type'):

        _cls = cls.get_child_class(base_cls, conf, type_tag_name=type_tag_name)
        kwargs = cls.get_setup_kwargs(conf)

        try:
            obj = _cls(**kwargs)
            return obj
        except TypeError as error:

            missing_argument = 'required positional argument' in error.args[0]
            unexpected_argument = 'unexpected keyword argument' in error.args[0]
            argsnames = error.args[0].split("'")[1::2]

            if missing_argument:
                raise YamlComposerException('class_init_missing_argument', base_cls=base_cls.__name__, args=argsnames)

            elif unexpected_argument:
                raise YamlComposerException('class_init_unexpected_argument', base_cls=base_cls.__name__, args=argsnames)

            else:
                raise

    @classmethod
    def get_child_class(cls, base_class, conf, type_tag_name='type'):

        _tag = conf.get(type_tag_name, False)
        if not _tag:
            raise YamlComposerException(
                'missing_configuration_options',
                option=type_tag_name,
                class_name=str(base_class)
            )

        child = base_class.by_type_tag(_tag)
        if not child:
            raise YamlComposerException(
                'tag_not_found',
                tag=_tag,
                class_name=str(base_class)
            )

        return child

    @classmethod
    def get_setup_kwargs(cls, conf):
        keywords = ['type', 'backup_policy', 'cleanup_policy', 'dispatchers', 'policy']
        return {key: value for key, value in conf.items() if key not in keywords}
