import pytest
import yaml
import json 

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from easybackup.core import exceptions as exp
from easybackup.core.configuration import Configuration, ConfigurationLoader, EasyBackupConfigurationError, ConfigurationProject
from easybackup.core.backup_creator import BackupCreator
from easybackup.core.backup_composer import BackupComposer
from easybackup.core.repository import Repository, RepositoryAdapter
from easybackup.core.repository_link import RepositoryLink, Synchroniser
from easybackup.policy.backup import BackupPolicy, TimeIntervalBackupPolicy
from easybackup.policy.cleanup import CleanupPolicy, LifetimeCleanupPolicy
from easybackup.i18n import i18n
from . import mock

import re


def is_number_version(string):
    return re.compile('[0-9]+\.[0-9]+\.[0-9]+').match(string)


ConfError = EasyBackupConfigurationError


class YamlConfigurationLoader(ConfigurationLoader):

    @classmethod
    def load(cls, document):
        obj = (yaml.load(document, Loader=Loader) or {})

        cls.check_version_number(obj)
        cls.check_projects(obj)
        cls.check_repositories(obj)

        repositories = cls.load_repositories(obj)
        composers = cls.load_backup_composers(obj, repositories)

        return Configuration(
            version=obj.get('version'),
            projects=cls.load_project(obj.get('projects')),
            composers=composers,
            repositories=repositories
        )

    @classmethod
    def check_version_number(cls, obj):
        version = obj.get('version')
        if (not version) or (type(version) is not str) or (not is_number_version(version)):
            raise ConfError('version_number_is_missing_or_invalid')

    @classmethod
    def check_projects(cls, obj):
        projects = obj.get('projects')
        if not projects:
            raise ConfError('configuration_should_have_at_least_one_project')

        if type(projects) is not dict:
            raise ConfError('projects_should_be_a_dictionnary')

    @classmethod
    def check_repositories(cls, obj):
        repositories = obj.get('repositories')
        if repositories and type(repositories) is not dict:
            raise ConfError('repositories_should_be_a_dictionnary')

    @classmethod
    def load_project(cls, projects):
        results = []
        for key_project, item in projects.items():

            volumes = []
            if type(item) is str:
                volumes = [item]
            elif type(item) is list:
                volumes = item
            elif type(item) is dict:
                volumes = item.keys()

            results.append(ConfigurationProject(
                name=key_project,
                volumes=volumes
            ))

        return results

    @classmethod
    def load_backup_composers(cls, obj, repositories):
        results = []
        for conf in cls.iter_volumes_configuration(obj):
            composer = cls.build_backup_composer(conf, repositories)
            results.append(composer)

        return results

    @classmethod
    def load_repositories(cls, obj):
        results = []

        repositories = obj.get('repositories', {})
        for name, conf in repositories.items():
            adapter = cls.build_object(RepositoryAdapter, conf)
            results.append(Repository(adapter, name=name))

        return results

    @classmethod
    def build_backup_composer(cls, conf, repositories):

        backup_policy = cls.build_object(BackupPolicy, conf['backup_policy'])
        creator = cls.build_object(BackupCreator, conf['creator'])

        cleanup_policy = False
        if conf['cleanup_policy']:
            cleanup_policy = cls.build_object(CleanupPolicy, conf['cleanup_policy'])

        synchronizers = []
        if conf['synchronizers']:

            source_adapter = creator.target_adapter()
            target_adapter = repositories[0].adapter

            link_cls = RepositoryLink.get_source_target_compatible(
                source=source_adapter.type_tag,
                target=target_adapter.type_tag
            )

            link = link_cls(source=source_adapter, target=target_adapter)

            synchronizers = [Synchroniser(link=link)]

        return BackupComposer(
            project=conf.get('project'),
            volume=conf.get('volume'),
            creator=creator,
            backup_policy=backup_policy,
            cleanup_policy=cleanup_policy,
            synchronizers=synchronizers
        )

    @classmethod
    def iter_volumes_configuration(cls, obj):
        projects = obj.get('projects')
        for project_name, volumes in projects.items():

            if type(volumes) is not dict:
                continue

            for volume_name, volume_conf in volumes.items():

                backup_policy = volume_conf.get('backup_policy')
                if not backup_policy:
                    raise ConfError('backup_policy_is_required', project=project_name, volume=volume_name)

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
    def build_object(cls, base_cls, conf):

        _cls = cls.get_child_class(base_cls, conf)
        kwargs = cls.get_setup_kwargs(conf)

        try:
            obj = _cls(**kwargs)
            return obj
        except TypeError as error:

            missing_argument = 'required positional argument' in error.args[0]
            unexpected_argument = 'unexpected keyword argument' in error.args[0]
            argsnames = error.args[0].split("'")[1::2]

            if missing_argument:
                raise ConfError('class_init_missing_argument', base_cls=base_cls.__name__, args=argsnames)

            elif unexpected_argument:
                raise ConfError('class_init_unexpected_argument', base_cls=base_cls.__name__, args=argsnames)

            else:
                raise


    @classmethod
    def get_child_class(cls, base_class, conf):

        _tag = conf.get('type', False)

        if not _tag:
            raise ConfError('MISSING_OPTIONS')

        child = base_class.by_type_tag(_tag)
        if not child:
            raise ConfError(
                'TAG_NOT_FOUND',
                tag=_tag,
                object=str(base_class)
            )

        return child

    @classmethod
    def get_setup_kwargs(cls, conf):
        keywords = ['type', 'backup_policy', 'cleanup_policy', 'dispatchers']
        return {key: value for key, value in conf.items() if key not in keywords}


def test_yaml_load_version():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myprojects:
    """)

    assert conf.version == '1.0.0'
    with pytest.raises(ConfError):
        YamlConfigurationLoader.load(""" """)

    with pytest.raises(ConfError):
        YamlConfigurationLoader.load("""version: a""")

    with pytest.raises(ConfError):
        YamlConfigurationLoader.load("""version: 1.2""")


def test_yaml_load_project():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject:
    """)

    assert len(conf.projects) == 1

    with pytest.raises(ConfError) as error:
        YamlConfigurationLoader.load("""version: 1.0.0""")
    assert error.value.code == 'configuration_should_have_at_least_one_project'

    with pytest.raises(ConfError) as error:
        conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject
    """)
    assert error.value.code == 'projects_should_be_a_dictionnary'

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject:
                - app
                - database
    """)
    assert 'app' in conf.projects[0].volumes
    assert 'database' in conf.projects[0].volumes


def test_yaml_load_multiple_composers():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B
                   backup_policy:
                     type: timeinterval
                     interval: 1000
                db:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B
                   backup_policy:
                     type: timeinterval
                     interval: 1000
    """)

    assert len(conf.composer) == 2


def test_load_backup_creator_by_type_tag():

    class TestBackupCreator(BackupCreator):
        type_tag = 'test'

    cls = BackupCreator.by_type_tag('test')
    assert cls is TestBackupCreator


def test_yaml_load_volume_creator_from_short_name():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B
                   backup_policy:
                     type: timeinterval
                     interval: 1000
    """)

    assert type(conf.composer[0]) is BackupComposer
    assert conf.composer[0].project == 'myproject'
    assert conf.composer[0].volume == 'app'
    assert conf.composer[0].creator.source_bucket == 'A'
    assert conf.composer[0].creator.target_bucket == 'B'


def test_load_backup_policy_by_type_tag():

    class TestBackupPolicy(BackupPolicy):
        type_tag = 'test'

    cls = BackupPolicy.by_type_tag('test')
    assert cls is TestBackupPolicy


def test_yaml_load_volume_creator_check_parameters():

    class TestBackupCreatorWithRequired(BackupCreator):
        type_tag = 'test_wt_required'

        def setup(self, required, notrequired=False):
            return 1

    with pytest.raises(ConfError) as error:
        YamlConfigurationLoader.load("""
            version: 1.0.0
            projects:
                myproject:
                    app:
                      type: test_wt_required
                      notrequired: ok
                      backup_policy:
                        type: timeinterval
                        interval: 1000
        """)
    assert error.value.code == 'class_init_missing_argument'

    with pytest.raises(ConfError) as error:
        YamlConfigurationLoader.load("""
            version: 1.0.0
            projects:
                myproject:
                    app:
                      type: test_wt_required
                      unexpected: ok
                      backup_policy:
                        type: timeinterval
                        interval: 1000
        """)
    assert error.value.code == 'class_init_unexpected_argument'


def test_yaml_load_volume_creator_with_backup_policy():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   backup_policy:
                     type: timeinterval
                     interval: 1000
                   source_bucket: A
                   target_bucket: B
    """)

    assert type(conf.composer[0].backup_policy) is TimeIntervalBackupPolicy
    assert conf.composer[0].backup_policy.interval == 1000


def test_yaml_load_volume_creator_with_cleanup_policy():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B

                   backup_policy:
                     type: timeinterval
                     interval: 1000

                   cleanup_policy:
                     type: lifetime
                     max_age: 1000
                     minimum: 5

    """)

    assert type(conf.composer[0].cleanup_policy) is LifetimeCleanupPolicy
    assert conf.composer[0].cleanup_policy.max_age == 1000
    assert conf.composer[0].cleanup_policy.minimum == 5


def test_yaml_load_repository():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0

        repositories:
            bucketB:
                type: inmemory
                bucket: B

            bucketC:
                type: inmemory
                bucket: C

        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B

                   backup_policy:
                     type: timeinterval
                     interval: 1000

    """)

    assert len(conf.repositories) == 2
    assert conf.repositories[0].name == 'bucketB'
    assert type(conf.repositories[0].adapter) is mock.MemoryRepositoryAdapter
    assert conf.repositories[0].adapter.bucket == 'B'

    assert conf.repositories[1].name == 'bucketC'
    assert conf.repositories[1].adapter.bucket == 'C'


def test_infer_repository_link_adapter_with_target_and_source():

    class TestInferTargetRep(RepositoryAdapter):
        type_tag = 'test_infer_target'

    class TestInferSourceRep(RepositoryAdapter):
        type_tag = 'test_infer_source'

    with pytest.raises(exp.RepositoryLinkNotFound):
        RepositoryLink.get_source_target_compatible(
            source='test_infer_target',
            target='test_infer_source'
        )

    class TestInferLinkRepository(RepositoryLink):
        type_tag_source = 'test_infer_target'
        type_tag_target = 'test_infer_source'

    cls_link = RepositoryLink.get_source_target_compatible(
        source='test_infer_target',
        target='test_infer_source'
    )

    assert cls_link == TestInferLinkRepository


def test_yaml_load_dispatcher():

    conf = YamlConfigurationLoader.load("""
        version: 1.0.0

        repositories:
            bucketB:
                type: inmemory
                bucket: B

        projects:
            myproject:
                app:
                    type: inmemory
                    source_bucket: A
                    target_bucket: B

                    backup_policy:
                        type: timeinterval
                        interval: 1000

                    dispatchers:
                        bucketB:

    """)

    composer = conf.composer[0]

    assert len(composer.synchronizers) == 1
    assert type(composer.synchronizers[0].link) is mock.MemoryRepositoryLink
    assert composer.synchronizers[0].link.target_adapter.bucket == 'B'
