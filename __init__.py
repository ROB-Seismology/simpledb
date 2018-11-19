"""
Module providing basic read-write access to SQL databases.
Currently supports MySQL, PostgreSQL and SQLite/SpatiaLite databases.

Author: Kris Vanneste, Royal Observatory of Belgium
"""

from __future__ import absolute_import, division, print_function, unicode_literals



## Make relative imports work in Python 3
import importlib


## Reloading mechanism
try:
	reloading
except NameError:
	## Module is imported for the first time
	reloading = False
else:
	## Module is reloaded
	reloading = True
	try:
		## Python 3
		from importlib import reload
	except ImportError:
		## Python 2
		pass


## Import submodules

## base
if not reloading:
	base = importlib.import_module('.base', package=__name__)
else:
	reload(base)
from .base import (SQLDB, SQLRecord, build_sql_query)

## sqlite, depends on base
if not reloading:
	sqlite = importlib.import_module('.sqlite', package=__name__)
else:
	reload(sqlite)
from .sqlite import (SQLiteDB, query_sqlite_db, query_sqlite_db_generic)
__all__ = base.__all__ + sqlite.__all__

## mysql, depends on base
if not reloading:
	mysql = importlib.import_module('.mysql', package=__name__)
else:
	reload(mysql)
if mysql.HAS_MYSQL:
	from .mysql import (MySQLDB, query_mysql_db, query_mysql_db_generic)
	__all__ += mysql.__all__

## postgres, depends on base
if not reloading:
	postgres = importlib.import_module('.postgres', package=__name__)
else:
	reload(postgres)
if postgres.HAS_POSTGRES:
	from .postgres import (PgSQLDB, query_pgsql_db, query_pgsql_db_generic)
	__all__ += postgres.__all__
