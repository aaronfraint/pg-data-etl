import click
from pg_data_etl.settings.make_config_file import make_config_file as _make_config_file


@click.group()
def main():
    "'pg' is used command-line access to the pg_data_etl library"
    pass


@click.command()
@click.option("--overwrite/--no-overwrite", default=False)
def make_config_file(overwrite):
    """Make a configuration file from the template"""
    _make_config_file(overwrite=overwrite)


all_commands = [make_config_file]

for cmd in all_commands:
    main.add_command(cmd)
