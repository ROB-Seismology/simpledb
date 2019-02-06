"""
Common read/write functionality for all SQL implementations
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
		## Hack because python2 sqlite does not accept a Unicode string in
		## row['string'] and raises "IndexError: Index must be int or string"
		return self._sql_rec.__getitem__(str(name))

	def __getattr__(self, name):
		return self.__getitem__(name)

	def get(self, name, default_value):
		if name in self._sql_rec:
			return self.__getitem__(name)
		else:
			return default_value

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

	connection = None
	verbose = False
	_placeholder = '%s'

	@abc.abstractmethod
	def connect(self):
		"""
		Method that creates a connection to the database and stores
		a handle in :prop:`connection`
		"""
		pass

	def get_cursor(self):
		return self.connection.cursor()

	def __del__(self):
		self.close()

	def _gen_sql_records(self, cursor):
		for row in cursor.fetchall():
			yield SQLRecord(row, self)

	def query_generic(self,
		query,
		values=(),
		verbose=False,
		print_table=False,
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
		:param print_table:
			bool, whether or not to print results of query in a table
			rather than returning records
			(default: False)
		:param errf:
			file object, where to print errors (default: None)

		:return:
			generator object, yielding an instance of :class:`SQLRecord`
			for each record
			or None if :param:`print_table` is True
		"""
		if errf != None:
			errf.write("%s\n" % query)
			errf.flush()
		elif verbose or self.verbose:
			print(query)

		cursor = self.get_cursor()
		cursor.execute(query, values)

		if print_table:
			import prettytable as pt
			## Ensure order of columns is preserved in table
			col_names = list(map(lambda x:x[0], cursor.description))
			tab = pt.PrettyTable(col_names)
			for r, rec in enumerate(self._gen_sql_records(cursor)):
				tab.add_row([rec[col_name] for col_name in col_names])
			print(tab)
		else:
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
		print_table=False,
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
		:param print_table:
		:param errf:
			see :meth:`query_generic`

		:return:
			generator object, yielding an instance of :class:`SQLRecord`
			for each record
		"""
		query = build_sql_query(table_clause, column_clause, join_clause,
								where_clause, having_clause, order_clause,
								group_clause)
		return self.query_generic(query, verbose=verbose,
								print_table=print_table,errf=errf)

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

	def list_table_columns(self,
		table_name):
		"""
		List column names in particular database table.

		:param table_name:
			str, name of database table

		:return:
			list of strings, column names
		"""
		query = "SELECT * FROM %s limit 0" % table_name
		cursor = self.get_cursor()
		cursor.execute(query)
		return [f[0] for f in cursor.description]

	def drop_table(self,
		table_name):
		"""
		Delete database table

		:param table_name:
			str, table name
		"""
		sql = 'DROP TABLE %s' % table_name
		self.query_generic(sql)
		self.connection.commit()

	def rename_table(self,
		table_name,
		new_table_name):
		"""
		Rename database table

		:param table_name:
			str, current table name
		:param new_table_name:
			str, new table name
		"""
		sql = 'ALTER TABLE %s RENAME TO %s'
		sql %= (table_name, new_table_name)
		self.query_generic(sql)
		self.connection.commit()

	def create_table(self,
		table_name,
		column_info_list):
		"""
		Create database table.

		:param table_name:
			str, table name
		:param column_info_list:
			list of column info specifications; these are either
			tuples (col_name, col_type, notnull, default_value, primary_key)
			or dictionaries with these keys.
			Check the relevant manuals to see which data types
			are supported.
			Note that only col_name and col_type are required.
		"""
		sql = 'CREATE TABLE %s(' % table_name
		for i, column_info in enumerate(column_info_list):
			if isinstance(column_info, (list, tuple)):
				colname, coltype, notnull, default_value, primary_key = column_info
			else:
				colname = column_info['col_name']
				coltype = column_info['col_type']
				notnull = column_info.get('notnull', 0)
				default_value = column_info.get('default_value')
				primary_key = column_info.get('primary_key', 0)
			if i != 0:
				sql += ', '
			sql += '%s %s' % (colname, coltype)
			if default_value:
				sql += ' default %s' % default_value
			if notnull:
				sql += ' NOT NULL'
			if primary_key:
				sql += ' PRIMARY KEY'
		sql += ')'
		self.query_generic(sql)
		self.connection.commit()

	def add_column(self,
		table_name,
		col_name,
		col_type,
		notnull=False,
		default_value=None,
		primary_key=False):
		"""
		Add column to database table.

		:param table_name:
			str, table name
		:param col_name:
			str, name of column
		:param col_type:
			str, column data type
		:param notnull:
			bool, whether or not column value is required not to be NULL
			(default: False)
		:param default_value:
			mixed, default value
			(default: None)
		:param primary_key:
			bool, whether or not column is a primary key
			(default: False)
		"""
		sql = 'ALTER TABLE %s ADD COLUMN %s %s'
		sql %= (table_name, col_name, col_type)
		if default_value:
			sql += ' default %s' % default_value
		if notnull:
			sql += ' NOT NULL'
		if primary_key:
			sql += ' PRIMARY KEY'
		self.query_generic(sql)
		self.connection.commit()

	def delete_column(self,
		table_name,
		col_name):
		"""
		Delete column from database.
		Not supported by SQLite!

		:param table_name:
			str, table name
		:param col_name:
			str, name of column
		"""
		sql = 'ALTER TABLE %s DROP COLUMN %s'
		sql %= (table_name, col_name)
		self.query_generic(sql)
		self.connection.commit()

	def add_records(self,
		table_name,
		recs,
		dry_run=False):
		"""
		Add records to database table.

		:param table_name:
			str, table name
		:param recs:
			list of dicts, mapping database table column names to values
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		cursor = self.get_cursor()
		for rec in recs:
			sql = "INSERT INTO %s (%s) VALUES (%s)"
			sql %= (table_name, ", ".join(rec.keys()), ', '.join([self._placeholder]*len(rec)))
			if self.verbose:
				print(sql)
			cursor.execute(sql, rec.values())

		if dry_run:
			self.connection.rollback()
		else:
			self.connection.commit()

	def delete_records(self,
		table_name,
		where_clause,
		dry_run=False):
		"""
		Delete records from table.

		:param table_name:
			string, name of database table
		:param where_clause:
			string, where clause identifying table records to delete.
			Note: if empty, all rows are deleted!!!
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		sql = 'DELETE FROM %s' % table_name
		if where_clause.lstrip()[:5].upper() == "WHERE":
			where_clause = where_clause.lstrip()[5:]
		if where_clause:
			sql += ' WHERE %s' % where_clause
		self.query_generic(sql)

		if dry_run:
			self.connection.rollback()
		else:
			self.connection.commit()

	def update_row(self,
		table_name,
		col_dict,
		where_clause,
		dry_run=False):
		"""
		Update values for a particular record in different columns

		:param table_name:
			string, name of database table
		:param col_dict:
			dict, mapping column names to values
		:param where_clause:
			string, where clause identifying table record
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		cursor = self.get_cursor()
		sql = 'UPDATE %s SET ' % table_name
		sql += ', '.join(['%s = %s' % (key, self._placeholder) for key in col_dict.keys()])

		if where_clause.lstrip()[:5].upper() == "WHERE":
			where_clause = where_clause.lstrip()[5:]
		if where_clause:
			sql += ' WHERE %s' % where_clause

		cursor.execute(sql, col_dict.values())

		if dry_run:
			self.connection.rollback()
		else:
			self.connection.commit()

	def update_rows(self,
		table_name,
		col_dict_list,
		id_col_name,
		dry_run=False):
		"""
		Update multiple records in a table

		:param table_name:
			string, name of database table
		:param col_dict_list:
			list of dicts, mapping column names to values
			or dict mapping column names to lists of values
		:param id_col_name:
			string, name of column in col_dict_list containing unique
			record ID (which will be used to build the where clause)
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		if isinstance(col_dict_list, list):
			for rec in col_dict_list:
				where_clause = '%s = %s' % (id_col_name, rec[id_col_name])
				rec = rec.copy()
				rec.pop(id_col_name)
				self.update_row(table_name, rec, where_clause)

		elif isinstance(col_dict_list, dict):
			# TODO: use update_column method if num_rows > num_cols
			col_names = dict.keys()
			col_names.pop(id_col_name)
			for i, id in enumerate(col_dict_list[id_col_name]):
				row_id = col_dict_list[id_col_name][i]
				where_clause = "%s = %s" % (id_col_name, row_id)
				rec = {col_name: col_dict_list[col_name][i] for col_name in col_names}
				self.update_row(table_name, rec, where_clause)

		if dry_run:
			self.connection.rollback()
		else:
			self.connection.commit()

	def create_index(self,
		table_name,
		col_name,
		idx_name=None):
		"""
		Create index on particular column of a database table.

		:param table_name:
			string, name of database table
		:param col_name:
			string, name of column
		:param idx_name:
			string, name of index
			(default: None)
		"""
		if not idx_name:
			idx_name = "%s_IDX" % col_name
		sql = "CREATE INDEX %s ON %s(%s)"
		sql %= (idx_name, table_name, col_name)
		self.query_generic(sql)
		self.connection.commit()
