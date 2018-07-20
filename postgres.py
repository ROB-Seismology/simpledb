"""
Provides read access to PostgreSQL databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys

from base import (SQLDB, SQLRecord, build_sql_query)


__all__ = ["PgSQLDB", "query_pgsql_db", "query_pgsql_db_generic"]


HAS_PSYCOPG2 = False
HAS_PG8000 = False

try:
	import psycopg2
except ImportError:
	try:
		import pg8000
	except:
		pass
	else:
		HAS_PG8000 = True
else:
	HAS_PSYCOPG2 = True

HAS_POSTGRES = HAS_PSYCOPG2 or HAS_PG8000

if HAS_POSTGRES:
	class PgSQLDB(SQLDB):
		"""
		Class representing PostgreSQL database.

		:param db:
			str, database name
		:param host:
			str, name of server where database is stored
		:param user:
			str, user name that can access the database
		:param passwd:
			str, password for user
		:param port:
			int, PostgreSQL server port number
			(default: 5432)
		"""
		def __init__(self, db, host, user, passwd, port=5432):
			self.db = db
			self.host = host
			self.user = user
			self.passwd = passwd
			self.port = port
			self.connect()

		def connect(self):
			if HAS_PSYCOPG2:
				self.connection = psycopg2.connect(host=self.host, user=self.user,
						password=self.passwd, database=self.db, port=self.port,
						cursor_factory=psycopg2.extras.DictCursor)
			else:
				self.connection = pg8000.connect(host=self.host, user=self.user,
						password=self.passwd, database=self.db, port=self.port)

		def _gen_sql_records(self, cursor):
			if HAS_PSYCOPG2:
				super(PgSQLDB, self)._gen_sql_records(cursor)
			else:
				## Manually convert each row to a dict
				fields = [rec[0] for rec in cursor.description]
				for row in cursor.fetchall():
					dict_row = {fields[i]: row[i] for i in range(len(fields))}
					yield SQLRecord(dict_row, self.db)

		def list_tables(self):
			cursor = self.get_cursor()
			query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'public'"
			cursor.execute(query)
			return [rec[0] for rec in cursor.fetchall()]


	def query_pgsql_db_generic(
		db,
		host,
		user,
		passwd,
		query,
		port=5432,
		verbose=False,
		errf=None):
		"""
		Generic query of Postgres database table, returning each record as a dict

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
			int, PostgreSQL server port number
			(default: 5432)
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors

		:return:
			generator object, yielding a dictionary for each record
		"""
		if HAS_PSYCOPG2:
			conn = psycopg2.connect(host=host, user=user, password=passwd, database=db,
					port=port, cursor_factory=psycopg2.extras.DictCursor)
			#c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
		else:
			conn = pg8000.connect(host=host, user=user, password=passwd, database=db,
					port=port)
		cur = conn.cursor()

		if errf !=None:
			errf.write("%s\n" % query)
			errf.flush()
		elif verbose:
			print(query)

		cur.execute(query)
		if HAS_PSYCOPG2:
			return cur.fetchall()
		else:
			## Manually convert each row to a dict
			def to_dict_cursor():
				fields = [rec[0] for rec in cur.description]
				num_fields = len(fields)
				for row in cur.fetchall():
					yield {fields[i]: row[i] for i in range(num_fields)}
			return to_dict_cursor()


	def query_pgsql_db(
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
		port=5432,
		verbose=False,
		errf=None):
		"""
		Read table from Postgres database, returning each record as a dict

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
			int, PostgreSQL server port number
			(default: 5432)
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors

		:return:
			generator object, yielding a dictionary for each record
		"""
		query = build_sql_query(table_clause, column_clause, join_clause,
						where_clause, having_clause, order_clause, group_clause)
		return query_pgsql_db_generic(db, host, user, passwd, query, port=port,
										verbose=verbose, errf=errf)



if __name__ == "__main__":
	from seismogisdb_secrets import (host, user, passwd)
	db = "nat_earth_cultural"
	pgdb = PgSQLDB(db, host, user, passwd)
	for rec in pgdb.query("ne_10m_admin_0_boundary_lines_land", verbose=True):
		pass
	print(rec.items())
	print(pgdb.list_tables())
