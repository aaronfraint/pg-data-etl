from __future__ import annotations
import subprocess
from pathlib import Path


def run_command_in_shell(command: str) -> str:
    """
    - Use subprocess to execute a command in a shell

    Arguments:
        command (str)

    Returns:
        str: output from the command that was run
    """

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    output, err = process.communicate()
    print(output.decode("utf-8"))

    if err:
        print(err.decode("utf-8"))

    process.kill()

    return output


class CommandPathManager:
    """
    The CommandPathManager class allows customization of all necessary
    CLI tools that this Python package builds on top of.

    It starts with the assumption that everything is on path, and that the
    default version of the command is the one you want to use.

    In some cases, you may have a `psql` installed in your Python environment
    that's incompatible with the database cluster you're connecting to. This class
    exists for this use case, where any and all commands can be configured to execute
    via an explicitly declared full path.

    This means you can alias 'psql' to something like '/my/custom/path/to/psql'

    Examples:
        >>> # use default behavior
        >>> cmd = CommandPathManager()
        >>> cmd.psql
        'psql'

        >>> # use custom path to psql
        >>> cmd = CommandPathManager(psql='/custom/path/to')
        >>> cmd.psql
        '/custom/path/to/psql'

    """

    def __init__(self, **kwargs):
        """
        - Define a dictionary of `bin_paths` for `psql` and `tippecanoe` as `None`
        - Assign any other bin paths provided at runtime
        """
        self.bin_paths = {"psql": None, "tippecanoe": None}

        self.set_bin_path(**kwargs)

    @property
    def configured_commands(self) -> list:
        """
        - Return a list of all command IDs that have been set
        """
        return list(self.bin_paths.keys())

    def _add_bin_path_if_exists(self, cmd: str, bin_id: str) -> str | None:
        """
        - Generate a full path for a `cmd` if the `bin_id` is configured.
        - Otherwise just return the `cmd` without a path, under the assumption that
        it is already on the user's system path

        Args:
            cmd (str): the command to be run from a terminal
            bin_id (str): the bin ID value to inherit the path from

        Returns:
            the full path or shorthand command
        """

        if bin_id in self.configured_commands:
            if self.bin_paths[bin_id]:
                return str(Path(self.bin_paths[bin_id]) / cmd)
            else:
                return cmd
        else:
            print(f"{bin_id=} is not an option. Use one of: {self.configured_commands}")
            return None

    def set_bin_path(self, **kwargs) -> None:
        """
        - Assign bin path to command using `cmd=bin_path` syntax
        """
        for k, v in kwargs.items():
            self.bin_paths[k] = v

    @property
    def psql(self):
        """
        - `psql` connects to a postgres db
        """
        return self._add_bin_path_if_exists("psql", bin_id="psql")

    @property
    def pg_dump(self):
        """
        - `pg_dump` exports a table or full database
        """
        return self._add_bin_path_if_exists("pg_dump", bin_id="psql")

    @property
    def shp2pgsql(self):
        """
        - `shp2pgsql` imports shapefiles into postgres
        """
        return self._add_bin_path_if_exists("shp2pgsql", bin_id="psql")

    @property
    def pgsql2shp(self):
        """
        - `pgsql2shp` exports spatial postgres data to shapefile
        """
        return self._add_bin_path_if_exists("pgsql2shp", bin_id="psql")

    @property
    def ogr2ogr(self):
        """
        - `ogr2ogr` converts spatial data between a variety of formats
        """
        return self._add_bin_path_if_exists("ogr2ogr", bin_id="psql")

    @property
    def tippecanoe(self):
        """
        - `tippecanoe` converts `.geojson` data into vector tiles
        - This is the only command that is not expected to exist in the `psql` bin
        """
        return self._add_bin_path_if_exists("tippecanoe", bin_id="tippecanoe")
