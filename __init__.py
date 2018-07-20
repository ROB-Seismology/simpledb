"""
Module providing simple read-only access to SQL databases as well as
write access to SQLite/SpatiaLite databases.
Currently supports MySQL, PostgreSQL and SQLite databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals


# Make relative imports work in Python 3.x
import os
import sys
import importlib
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

try:
    reloading
except NameError:
    reloading = False # means the module is being imported
else:
    reloading = True # means the module is being reloaded

if not reloading:
	import base
else:
	importlib.reload(base)
from base import (SQLDB, SQLRecord, build_sql_query)

if not reloading:
	import sqlite
else:
	importlib.reload(sqlite)
from sqlite import (SQLiteDB, query_sqlite_db, query_sqlite_db_generic)

if not reloading:
	import mysql
else:
	importlib.reload(mysql)
if mysql.HAS_MYSQL:
	from mysql import (MySQLDB, query_mysql_db, query_mysql_db_generic)

if not reloading:
	import postgres
else:
	importlib.reload(postgres)
if postgres.HAS_POSTGRES:
	from postgres import (PgSQLDB, query_pgsql_db, query_pgsql_db_generic)
