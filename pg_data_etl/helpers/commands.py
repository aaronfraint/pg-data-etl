from __future__ import annotations
import subprocess
from pathlib import Path


def run_command_in_shell(command: str) -> str:
    """
    - Use subprocess to execute a command in a shell

    Args:
        command (str)

    TODO: docstring
    """

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    output = process.communicate()
    print(output[0])

    process.kill()

    return output


class CommandPathManager:
    """
    TODO: docstring
    """

    def __init__(self, psql_bin: str | None = None, tippecanoe_bin: str | None = None):
        self.bins = {"psql": psql_bin, "tippecanoe": tippecanoe_bin}

    def _add_bin_path_if_exists(self, cmd, bin: str = "psql") -> Path:
        """
        TODO: docstring
        """

        if bin in self.bins.keys():
            if not self.bins[bin]:
                return cmd
            else:
                return Path(self.bins[bin]) / cmd
        else:

            print(f"{bin=} is not an option. Use one of: {self.bins.keys()}")
            return None

    def set_bin_path(self, **kwargs) -> None:
        """
        - assign bin path to command using `cmd=bin_path` syntax
        """
        for k, v in kwargs.items():
            self.bins[k] = v

    @property
    def psql(self):
        return self._add_bin_path_if_exists("psql")

    @property
    def pg_dump(self):
        return self._add_bin_path_if_exists("pg_dump")

    @property
    def shp2pgsql(self):
        return self._add_bin_path_if_exists("shp2pgsql")

    @property
    def pgsql2shp(self):
        return self._add_bin_path_if_exists("pgsql2shp")

    @property
    def ogr2ogr(self):
        return self._add_bin_path_if_exists("ogr2ogr")

    @property
    def tippecanoe(self):
        return self._add_bin_path_if_exists("tippecanoe", "tippecanoe")
