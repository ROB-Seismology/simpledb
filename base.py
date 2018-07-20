"""
Module providing simple read-only access to SQL databases as well as
write access to SQLite/SpatiaLite databases.
Currently supports MySQL, PostgreSQL and SQLite databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import abc


__all__ = ["SQLDB", "SQLRecord", "build_sql_query"]


def build_sql_query(
	table_clause,
	column_clause="*",
	join_clause="",
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

	:return:
		str, SQL query
	"""
	if isinstance(table_clause, (list, tuple)):
		table_clause = ', '.join(table_clause)
	if isinstance(column_clause, (list, tuple)):
		column_clause = ', '.join(column_clause)
	query = 'SELECT %s FROM %s' % (column_clause, table_clause)
	if join_clause:
		if isinstance(join_clause, (list, tuple)):
			for (join_type, join_table, condition) in join_clause:
				if not join_type.split()[-1] == "JOIN":
					join_type += ' JOIN'
				query += ' %s %s ON %s' % (join_type.upper(), join_table, condition)
		else:
			query += ' %s' % join_clause
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

	def to_dict(self):
		return {key:val for key,val in self.items()}

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
		values=(),
		verbose=False,
		errf=None):
		"""
		Generic query of one or more database tables.

		:param query:
			str, SQL query
		:param values:
			tuple or dict, containing values or named parameters to be
			substituted in the query
			(default: ())
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
			print(query)

		cursor = self.get_cursor()
		cursor.execute(query, values)

		def to_sql_record():
			for row in cursor.fetchall():
				yield SQLRecord(row, self)

		return self._gen_sql_records(cursor)

	def query(self,
		table_clause,
		column_clause="*",
		join_clause="",
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
		:param verbose:
			bool, whether or not to print the query (default: False)
		:param errf:
			file object, where to print errors (default: None)

		:return:
			generator object, yielding an instance of :class:`SQLRecord`
			for each record
		"""
		query = build_sql_query(table_clause, column_clause, join_clause,
								where_clause, having_clause, order_clause)
		return self.query_generic(query, verbose=verbose, errf=errf)

	def get_num_rows(self, table_name):
		"""
		Determine number of rows in database table.

		:param table_name:
			str, table name

		:return:
			int, number of rows
		"""
		query = 'SELECT COUNT(*) as count FROM %s' % table_name
		return list(self.query_generic(query))[0]['count']

	def close(self):
		self.connection.close()

