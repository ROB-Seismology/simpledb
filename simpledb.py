"""
Module providing simple read-only access to SQL databases.
Currently supports MySQL, PostgreSQL and SQLite databases.
"""

import abc


def build_sql_query(
	table_clause,
	column_clause="*",
	where_clause="",
	having_clause="",
	order_clause="",
	group_clause=""):
	"""
	Build SQL query from component clauses

	:param table_clause:
		str or list of strings, name(s) of database table(s)
	:param column_clause:
		str or list of strings, column clause or list of columns (default: "*")
	:param where_clause:
		str, where clause (default: "")
	:param having_clause:
		str, having clause (default: "")
	:param order_clause:
		str, order clause (default: "")
	:param group_clause:
		str, group clause (default: "")

	:return:
		str, SQL query
	"""
	if isinstance(table_clause, (list, tuple)):
		table_clause = ', '.join(table_clause)
	if isinstance(column_clause, (list, tuple)):
		column_clause = ', '.join(column_clause)
	query = 'SELECT %s from %s' % (column_clause, table_clause)
	if where_clause:
		if where_clause.lstrip()[:5].upper() == "WHERE":
			where_clause = where_clause.lstrip()[5:]
		query += ' WHERE %s' % where_clause
	if group_clause:
		if group_clause.lstrip()[:8].upper() == "GROUP BY":
			group_clause = group_clause.lstrip()[8:]
		query += ' GROUP BY %s' % group_clause
	if having_clause:
		if having_clause.lstrip()[:6].upper() == "HAVING":
			having_clause = having_clause.lstrip()[6:]
		query += ' HAVING %s' % having_clause
	if order_clause:
		if order_clause.lstrip()[:8].upper() == "ORDER BY":
			order_clause = order_clause.lstrip()[8:]
		query += ' ORDER BY %s' % order_clause

	return query


class SQLRecord(object):
	"""
	Class representing a record from an SQL database.

	:param sql_rec:
		SQL record as returned from a dictionary cursor
	:param db:
		instance of :class:`SQLDB`
	"""
	def __init__(self, sql_rec, db):
		self._sql_rec = sql_rec
		self.db = db

	def __getitem__(self, name):
		return self._sql_rec[name]

	def __getattr__(self, name):
		return self._sql_rec[name]

	def keys(self):
		return self._sql_rec.keys()

	def values(self):
		return [self._sql_rec[key] for key in self.keys()]

	def items(self):
		return [(key, val) for key, val in zip(self.keys(), self.values())]

	def update(self, table_name):
		pass

	def insert(self, table_name):
		pass


class SQLDB(object):
	"""
	Abstract base class for SQL database connections.
	"""
	__metaclass__ = abc.ABCMeta

	@abc.abstractmethod
	def connect(self):
		"""
		Method that creates a connection to the database and stores
		a handle in :prop:`connection`
		"""
		pass

	def get_cursor(self):
		return self.connection.cursor()

	def _gen_sql_records(self, cursor):
		for row in cursor.fetchall():
			yield SQLRecord(row, self)

	def query_generic(self,
		query,
		verbose=False,
		errf=None):
		"""
		Generic query of one or more database tables.

		:param query:
			str, SQL query
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors (default: None)

		:return:
			generator object, yielding an instance of :class:`SQLRecord`
			for each record
		"""
		if errf != None:
			errf.write("%s\n" % query)
			errf.flush()
		elif verbose:
			print query

		cursor = self.get_cursor()
		cursor.execute(query)

		def to_sql_record():
			for row in cursor.fetchall():
				yield SQLRecord(row, self)

		return self._gen_sql_records(cursor)

	def query(self,
		table_clause,
		column_clause="*",
		where_clause="",
		having_clause="",
		order_clause="",
		group_clause="",
		verbose=False,
		errf=None):
		"""
		Query one or more database tables using separate clauses.

		:param table_clause:
			str or list of strings, name(s) of database table(s)
		:param column_clause:
			str or list of strings, column clause or list of columns (default: "*")
		:param where_clause:
			str, where clause (default: "")
		:param having_clause:
			str, having clause (default: "")
		:param order_clause:
			str, order clause (default: "")
		:param group_clause:
			str, group clause (default: "")
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors (default: None)

		:return:
			generator object, yielding an instance of :class:`SQLRecord`
			for each record
		"""
		query = build_sql_query(table_clause, column_clause, where_clause,
								having_clause, order_clause)
		return self.query_generic(query, verbose=verbose, errf=errf)


## sqlite
import sqlite3


class SQLiteDB(SQLDB):
	"""
	Class representing SQLite database.

	:param db_filespec:
		str, full path to sqlite database
	"""
	def __init__(self, db_filespec):
		self.db_filespec = db_filespec
		self.connect()

	def connect(self):
		self.connection = sqlite3.connect(db_filespec)
		self.connection.row_factory = sqlite3.Row

	def list_tables(self):
		query = "SELECT * FROM sqlite_master WHERE type='table'"
		return [rec.name for rec in self.query_generic(query)]


def query_sqlite_db_generic(
	db_filespec,
	query,
	verbose=False):
	"""
	Read table from sqlite database, returning each record as a dict

	:param db_filespec:
		str, full path to sqlite database
	:param query:
		str, SQL query
	:param verbose:
		bool, whether or not to print the query (default: False)

	:return:
		generator object, yielding a dictionary for each record
	"""
	db = sqlite3.connect(db_filespec)
	db.row_factory = sqlite3.Row
	cur = db.cursor()
	if verbose:
		print query
	cur.execute(query)
	return cur.fetchall()


def query_sqlite_db(
	db_filespec,
	table_clause,
	column_clause="*",
	where_clause="",
	having_clause="",
	order_clause="",
	group_clause="",
	verbose=False):
	"""
	Read table from sqlite database, returning each record as a dict

	:param db_filespec:
		str, full path to sqlite database
	:param table_clause:
		str or list of strings, name(s) of database table(s)
	:param column_clause:
		str or list of strings, column clause or list of columns (default: "*")
	:param where_clause:
		str, where clause (default: "")
	:param having_clause:
		str, having clause (default: "")
	:param order_clause:
		str, order clause (default: "")
	:param group_clause:
		str, group clause (default: "")
	:param verbose:
		bool, whether or not to print the query (default: False)

	:return:
		generator object, yielding a dictionary for each record
	"""
	query = build_sql_query(table_clause, column_clause, where_clause,
							having_clause, order_clause)
	return query_sqlite_db_generic(db_filespec, query, verbose=verbose)


## Mysql
try:
	import MySQLdb
except ImportError:
	try:
		import pymysql as MySQLdb
	except ImportError:
		HAS_MYSQL = False
	else:
		HAS_MYSQL = True
else:
	HAS_MYSQL = True


if HAS_MYSQL:
	import MySQLdb.cursors

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
					cursorclass=MySQLdb.cursors.DictCursor, use_unicode=True)

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
				port=port, cursorclass=MySQLdb.cursors.DictCursor, use_unicode=True)
		cur = conn.cursor()

		if errf !=None:
			errf.write("%s\n" % query)
			errf.flush()
		elif verbose:
			print query

		cur.execute(query)
		return cur.fetchall()


	def query_mysql_db(
		db,
		host,
		user,
		passwd,
		table_clause,
		column_clause="*",
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
		query = build_sql_query(table_clause, column_clause, where_clause,
								having_clause, order_clause, group_clause)
		return query_mysql_db_generic(db, host, user, passwd, query, port=port,
										verbose=verbose, errf=errf)


## Postgres
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

if HAS_PSYCOPG2 or HAS_PG8000:
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
			print query

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
		query = build_sql_query(table_clause, column_clause, where_clause,
								having_clause, order_clause, group_clause)
		return query_pgsql_db_generic(db, host, user, passwd, query, port=port,
										verbose=verbose, errf=errf)



if __name__ == "__main__":
	import os
	db_filespec = r"C:\Users\kris\ownCloud\Mendeley Desktop\kris.vanneste@oma.be@www.mendeley.com.sqlite"
	if os.path.exists(db_filespec):
		sqldb = SQLiteDB(db_filespec)
		print sqldb.list_tables()
		for rec in sqldb.query("Documents", verbose=True):
			pass
		print rec.items()

	from seismogisdb_secrets import (host, user, passwd)
	db = "nat_earth_cultural"
	pgdb = PgSQLDB(db, host, user, passwd)
	for rec in pgdb.query("ne_10m_admin_0_boundary_lines_land", verbose=True):
		pass
	print rec.items()
	print pgdb.list_tables()
