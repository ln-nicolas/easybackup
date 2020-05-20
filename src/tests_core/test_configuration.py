import pytest

from easybackup.core import exceptions as exp
from easybackup.core.backup_creator import BackupCreator
from easybackup.core.backup_supervisor import BackupSupervisor
from easybackup.core.repository import RepositoryAdapter
from easybackup.core.repository_link import RepositoryLink
from easybackup.policy.backup import BackupPolicy, TimeIntervalBackupPolicy
from easybackup.policy.cleanup import LifetimeCleanupPolicy
from easybackup.policy.synchronization import SynchronizeRecentPolicy
from easybackup.loader.yaml_composer import YamlComposer, YamlComposerException
from easybackup.core.lexique import parse_time_duration


from . import mock


def test_yaml_check_error_version():

    with pytest.raises(YamlComposerException) as error:
        YamlComposer("""version: 1.0.0""")
    assert error.value.code == 'configuration_should_have_at_least_one_project'


def test_yaml_check_error_project_definition():

    with pytest.raises(YamlComposerException) as error:
        YamlComposer("""
        version: 1.0.0
        projects:
            myproject
    """)
    assert error.value.code == 'projects_should_be_a_dictionnary'


def test_yaml_load_multiple_composers():

    conf = YamlComposer("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B
                   backup_policy:
                     policy: timeinterval
                     interval: 1000
                db:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B
                   backup_policy:
                     policy: timeinterval
                     interval: 1000
    """)

    assert len(conf.composers) == 2


def test_load_backup_creator_by_type_tag():

    class TestBackupCreator(BackupCreator):
        type_tag = 'test'

    cls = BackupCreator.by_type_tag('test')
    assert cls is TestBackupCreator


def test_load_time_interval_from_formated_string():

    assert parse_time_duration('10000') == 10000
    assert parse_time_duration('10000s') == 10000
    assert parse_time_duration('1m') == 60
    assert parse_time_duration('1m10s') == 70
    assert parse_time_duration('1h') == 60*60
    assert parse_time_duration('1h30m10s') == 60*60 + 30*60 + 10
    assert parse_time_duration('1d') == 60*60*24
    assert parse_time_duration('1d1h30m10s') == 60*60*24 + 60*60 + 30*60 + 10
    assert parse_time_duration('1d10s') == 60*60*24 + 10

    with pytest.raises(Exception):
        parse_time_duration('abcd')

    with pytest.raises(Exception):
        parse_time_duration('-10s')


def test_yaml_load_volume_creator_from_short_name():

    composers = YamlComposer("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B
                   backup_policy:
                     policy: timeinterval
                     interval: 1000
    """).composers

    assert type(composers[0]) is BackupSupervisor
    assert composers[0].project == 'myproject'
    assert composers[0].volume == 'app'
    assert composers[0].creator.source_bucket == 'A'
    assert composers[0].creator.target_bucket == 'B'


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

    with pytest.raises(YamlComposerException) as error:
        YamlComposer("""
            version: 1.0.0
            projects:
                myproject:
                    app:
                      type: test_wt_required
                      notrequired: ok
                      backup_policy:
                        policy: timeinterval
                        interval: 1000
        """).composers
    assert error.value.code == 'class_init_missing_argument'

    with pytest.raises(YamlComposerException) as error:
        YamlComposer("""
            version: 1.0.0
            projects:
                myproject:
                    app:
                      type: test_wt_required
                      unexpected: ok
                      backup_policy:
                        policy: timeinterval
                        interval: 1000
        """).composers
    assert error.value.code == 'class_init_unexpected_argument'


def test_yaml_load_volume_creator_with_backup_policy():

    composers = YamlComposer("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   backup_policy:
                     policy: timeinterval
                     interval: 1000
                   source_bucket: A
                   target_bucket: B
    """).composers

    assert type(composers[0].backup_policy) is TimeIntervalBackupPolicy
    assert composers[0].backup_policy.interval == 1000


def test_yaml_load_volume_creator_with_cleanup_policy():

    composers = YamlComposer("""
        version: 1.0.0
        projects:
            myproject:
                app:
                   type: inmemory
                   source_bucket: A
                   target_bucket: B

                   backup_policy:
                     policy: timeinterval
                     interval: 1000

                   cleanup_policy:
                     policy: lifetime
                     max_age: 1000
                     minimum: 5

    """).composers

    assert type(composers[0].cleanup_policy) is LifetimeCleanupPolicy
    assert composers[0].cleanup_policy.max_age == 1000
    assert composers[0].cleanup_policy.minimum == 5


def test_yaml_load_repository():

    loader = YamlComposer("""
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
                     policy: timeinterval
                     interval: 1000
    """)

    repositories = loader.repositories

    assert repositories[0].name == 'bucketB'
    assert type(repositories[0].adapter) is mock.MemoryRepositoryAdapter
    assert repositories[0].adapter.bucket == 'B'

    assert repositories[1].name == 'bucketC'
    assert repositories[1].adapter.bucket == 'C'


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

    composers = YamlComposer("""
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
                        policy: timeinterval
                        interval: 1000

                    dispatchers:
                        bucketB:
                            policy: recent
                            minimum: 5
                        bucketC:
                            policy: recent
                            minimum: 1
    """).composers

    composer = composers[0]

    assert len(composer.synchronizers) == 2

    assert type(composer.synchronizers[0].link) is mock.MemoryRepositoryLink
    assert composer.synchronizers[0].link.target_adapter.bucket == 'B'

    assert composer.synchronizers[1].link.target_adapter.bucket == 'C'

    assert type(composer.synchronizers[0].sync_policy) is SynchronizeRecentPolicy
    assert composer.synchronizers[0].sync_policy.minimum == 5
