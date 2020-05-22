
class i18n():

    messages = {
        'version_number_is_missing_or_invalid': 'Version number is missing or invalid.',
        'configuration_should_have_at_least_one_project': 'Configuration should have at least one project',
        'projects_should_be_a_dictionnary': 'projects should be a dictionnary.',
        'backup_policy_is_required': 'Backup policy is required in {project}/{volume}.',
        'class_init_missing_argument': 'Missing argument ({args}) initializing {base_cls}.',
        'class_init_unexpected_argument': 'Unexpected argument ({args}) initializing {base_cls}.',
        'repositories_should_be_a_dictionnary': 'repositories should be a dictionnary.',
        'time_do_not_match_time_format': '{string} do not match time format (dhms)',
        'could_not_find_compatible_connector': 'Could not find compatible connector between {source} and {target}.',
        'tag_not_found': 'Could not find tag: {tag} matching class: {class_name}.',
        'missing_configuration_options': 'Missing configuration option {option} for class: {class_name}'
    }

    @classmethod
    def t(cls, key, **kwargs):
        return cls.messages.get(key).format(**kwargs)
