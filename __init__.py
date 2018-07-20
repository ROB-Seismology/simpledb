"""
Module providing simple read-only access to SQL databases as well as
write access to SQLite/SpatiaLite databases.
Currently supports MySQL, PostgreSQL and SQLite databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals


# Make relative imports work in Python 3.x
import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


import base
#reload(base)
from base import (SQLDB, SQLRecord, build_sql_query)

import sqlite
#reload(sqlite)
from sqlite import (SQLiteDB, query_sqlite_db, query_sqlite_db_generic)

import mysql
#reload(mysql)
if mysql.HAS_MYSQL:
	from mysql import (MySQLDB, query_mysql_db, query_mysql_db_generic)

import postgres
#reload(postgres)
if postgres.HAS_POSTGRES:
	from postgres import (PgSQLDB, query_pgsql_db, query_pgsql_db_generic)
