"""
Provides read access to MySQL databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys

from base import (SQLDB, SQLRecord, build_sql_query)


__all__ = ["MySQLDB", "query_mysql_db", "query_mysql_db_generic"]


HAS_MYSQLDB = HAS_PYMYSQL = False

try:
	import MySQLdb
except ImportError:
	try:
		import pymysql as MySQLdb
	except ImportError:
		HAS_PYMYSQL = False
	else:
		HAS_PYMYSQL = True
else:
	HAS_MYSQLDB = True

HAS_MYSQL = HAS_MYSQLDB or HAS_PYMYSQL

if HAS_MYSQL:
	if HAS_MYSQLDB:
		from MySQLdb import cursors
	else:
		from pymysql import cursors

	class MySQLDB(SQLDB):
		"""
		Class representing MySQL database.

		:param db:
			str, database name
		:param host:
			str, name of server where database is stored
		:param user:
			str, user name that can access the database
		:param passwd:
			str, password for user
		:param port:
			int, MySQL server port number
			(default: 3306)
		"""
		def __init__(self, db, host, user, passwd, port=3306):
			self.db = db
			self.host = host
			self.user = user
			self.passwd = passwd
			self.port = port
			self.connect()

		def connect(self):
			self.connection = MySQLdb.connect(host=self.host, user=self.user,
					passwd=self.passwd, db=self.db, port=self.port,
					cursorclass=cursors.DictCursor, use_unicode=True)

		def list_tables(self):
			cursor = self.get_cursor()
			#cursor.execute("USE %s" % self.db)
			cursor.execute("SHOW TABLES")
			return [rec.values()[0] for rec in cursor.fetchall()]


	def query_mysql_db_generic(
		db,
		host,
		user,
		passwd,
		query,
		port=3306,
		verbose=False,
		errf=None):
		"""
		Generic query of MySQL database table, returning each record as a dict

		:param db:
			str, database name
		:param host:
			str, name of server where database is stored
		:param user:
			str, user name that can access the database
		:param passwd:
			str, password for user
		:param query:
			str, SQL query
		:param port:
			int, MySQL server port number
			(default: 3306)
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors

		:return:
			generator object, yielding a dictionary for each record
		"""
		conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db,
				port=port, cursorclass=cursors.DictCursor, use_unicode=True)
		cur = conn.cursor()

		if errf !=None:
			errf.write("%s\n" % query)
			errf.flush()
		elif verbose:
			print(query)

		cur.execute(query)
		return cur.fetchall()


	def query_mysql_db(
		db,
		host,
		user,
		passwd,
		table_clause,
		column_clause="*",
		join_clause="",
		where_clause="",
		having_clause="",
		order_clause="",
		group_clause="",
		port=3306,
		verbose=False,
		errf=None):
		"""
		Read table from MySQL database, returning each record as a dict

		:param db:
			str, database name
		:param host:
			str, name of server where database is stored
		:param user:
			str, user name that can access the database
		:param passwd:
			str, password for user
		:param table_clause:
			str or list of strings, name(s) of database table(s)
		:param column_clause:
			str or list of strings, column clause or list of columns (default: "*")
		:param join_clause:
			str or list of (join_type, table_name, condition) tuples,
			join clause (default: "")
		:param where_clause:
			str, where clause (default: "")
		:param having_clause:
			str, having clause (default: "")
		:param order_clause:
			str, order clause (default: "")
		:param group_clause:
			str, group clause (default: "")
		:param port:
			int, MySQL server port number
			(default: 3306)
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors
			(default: None)

		:return:
			generator object, yielding a dictionary for each record
		"""
		query = build_sql_query(table_clause, column_clause, join_clause,
						where_clause, having_clause, order_clause, group_clause)
		return query_mysql_db_generic(db, host, user, passwd, query, port=port,
										verbose=verbose, errf=errf)



if __name__ == "__main__":
	pass
