"""
Provides access to MySQL databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys

from .base import (SQLDB, SQLRecord, build_sql_query)


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
		from pymysql import cursors
		HAS_PYMYSQL = True
else:
	from MySQLdb import cursors
	HAS_MYSQLDB = True

HAS_MYSQL = HAS_MYSQLDB or HAS_PYMYSQL


if HAS_MYSQL:
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

		def __del__(self):
			self.connection.close()

		def connect(self):
			self.connection = MySQLdb.connect(host=self.host, user=self.user,
					passwd=self.passwd, db=self.db, port=self.port,
					cursorclass=cursors.DictCursor, use_unicode=True)

		def list_tables(self):
			cursor = self.get_cursor()
			#cursor.execute("USE %s" % self.db)
			cursor.execute("SHOW TABLES")
			return [rec.values()[0] for rec in cursor.fetchall()]

		def get_column_info(self,
			table_name):
			"""
			Return column info for particular table
			:param table_name:
				str, name of database table

			:return:
				list of dictionaries, one for each column, with following keys:
				- Field: column name
				- Type: column data type
				- Null: whether or not the column can be NULL
				- Key: key type
				- Default: default value
				- Extra:
			"""
			query = "DESCRIBE %s" % table_name
			cursor = self.get_cursor()
			cursor.execute(query)
			return [{key: row[key] for key in row.keys()} for row in cursor.fetchall()]

		def vacuum(self,
			table_name):
			"""
			Clean empty records from database or database table.

			:param table_name:
				string, name of database table
			"""
			query = "OPTIMIZE TABLE %s" % table_name
			self.query_generic(query)

		def close(self):
			"""
			Close database connection
			"""
			self.connection.close()


	def query_mysql_db_generic(
		db,
		host,
		user,
		passwd,
		query,
		port=3306,
		verbose=False,
		print_table=False,
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
		:param print_table:
			bool, whether or not to print results of query in a table
			rather than returning records
			(default: False)
		:param errf:
			file object, where to print errors

		:return:
			generator object, yielding a dictionary for each record
		"""
		if print_table:
			cursor_class = cursors.Cursor
		else:
			cursor_class = cursors.DictCursor
		conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db,
				port=port, cursorclass=cursor_class, use_unicode=True)
		cur = conn.cursor()

		if errf !=None:
			errf.write("%s\n" % query)
			errf.flush()
		elif verbose:
			print(query)

		cur.execute(query)
		if print_table:
			import prettytable as pt
			tab = pt.from_db_cursor(cur)
			print(tab)
		else:
			return cur.fetchall()
		cur.close()
		conn.close()


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
		print_table=False,
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
		:param print_table:
		:param errf:
			see :func:`query_mysql_db_generic`

		:return:
			generator object, yielding a dictionary for each record
		"""
		query = build_sql_query(table_clause, column_clause, join_clause,
						where_clause, having_clause, order_clause, group_clause)
		return query_mysql_db_generic(db, host, user, passwd, query, port=port,
							verbose=verbose, print_table=print_table, errf=errf)



if __name__ == "__main__":
	pass
