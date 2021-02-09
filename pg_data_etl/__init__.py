from .db import Database, Query
from .configuration_manager import configurations as _configurations

connections = _configurations()
