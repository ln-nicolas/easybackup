import argparse
import sys

from easybackup import __version__
from easybackup.loader.yaml_composer import YamlComposer

def parse_args():
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Easybackup cli")
    parser.add_argument(
        "--version",
        action="version",
        version="my_project {ver}".format(ver=__version__))
    parser.add_argument(
        "composer",
        help="backup-compose filename",
        type=str,
    )
    args = parser.parse_args()
    return args


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args()

    composer_file = open(args.composer, 'r')
    composer_content = composer_file.read()

    conf = YamlComposer(composer_content)
    for composer in conf.composers:
        composer.run()


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
