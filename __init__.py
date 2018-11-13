"""
Module providing basic read-write access to SQL databases.
Currently supports MySQL, PostgreSQL and SQLite/SpatiaLite databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals


## Make relative imports (in submodules) work in Python 3.x
import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


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
		from importlib import reload
	except ImportError:
		pass

## Import submodules

## base
if not reloading:
	import db.simpledb.base
else:
	reload(db.simpledb.base)
	#reload(sys.modules[__name__ + '.' + 'base'])
from db.simpledb.base import (SQLDB, SQLRecord, build_sql_query)

## sqlite, depends on base
if not reloading:
	import db.simpledb.sqlite
else:
	reload(db.simpledb.sqlite)
from db.simpledb.sqlite import (SQLiteDB, query_sqlite_db, query_sqlite_db_generic)
## Don't know why this works, and absolute module paths do not...
__all__ = base.__all__ + sqlite.__all__

## mysql, depends on base
if not reloading:
	import db.simpledb.mysql
else:
	reload(db.simpledb.mysql)
if mysql.HAS_MYSQL:
	from db.simpledb.mysql import (MySQLDB, query_mysql_db, query_mysql_db_generic)
	__all__ += mysql.__all__

## postgres, depends on base
if not reloading:
	import db.simpledb.postgres
else:
	reload(db.simpledb.postgres)
if postgres.HAS_POSTGRES:
	from db.simpledb.postgres import (PgSQLDB, query_pgsql_db, query_pgsql_db_generic)
	__all__ += postgres.__all__

## Remove module root folder from sys.path
sys.path = sys.path[:-1]
