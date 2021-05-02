"""
TODO: docstring
"""

from pathlib import Path

DB_CONFIG_FILEPATH = Path.home() / ".pg-data-etl" / "database_connections.cfg"

STARTER_CONFIG_FILE = """
[DEFAULT]
pw = this-is-a-placeholder-password
port = 5432
super_db = postgres
super_un = postgres
super_pw = this-is-another-placeholder-password

[localhost]
host = localhost
un = postgres
pw = your-password-here

[digitalocean]
un = your-username-here
host = your-host-here.db.ondigitalocean.com
pw = your-password-here
port = 98765
sslmode = require
super_db = your_default_db
super_un = your_super_admin
super_pw = some_super_password12354
"""