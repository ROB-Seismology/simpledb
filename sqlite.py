"""
Provides read/write access to SQLite databases.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys

from base import (SQLDB, SQLRecord, build_sql_query)

import sqlite3


__all__ = ["SQLiteDB", "query_sqlite_db", "query_sqlite_db_generic"]


class SQLiteDB(SQLDB):
	"""
	Class representing SQLite database.

	:param db_filespec:
		str, full path to sqlite database (can also be ':memory:')
	"""
	def __init__(self, db_filespec):
		self.db_filespec = db_filespec
		self.connect()

	def connect(self):
		self.connection = sqlite3.connect(self.db_filespec, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self.connection.row_factory = sqlite3.Row

		## Enable spatialite extension if possible
		spatialite_path = os.path.split(sys.executable)[0]
		spatialite_path = os.path.join(spatialite_path, "GDAL", "mod-spatialite")
		os.environ["PATH"] += ";%s" % spatialite_path
		self.connection.enable_load_extension(True)
		try:
			self.connection.load_extension('mod_spatialite.dll')
		except:
			print("Warning: mod_spatialite.dll could not be loaded!")
			self.has_spatialite = False
		else:
			self.has_spatialite = True

	def list_tables(self):
		"""
		List database tables.

		return:
			list of strings, names of database tables
		"""
		query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY NAME"
		return [rec.name for rec in self.query_generic(query)]

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

	def get_column_info(self,
		table_name):
		"""
		Return column info for particular table
		:param table_name:
			str, name of database table

		:return:
			list of dictionaries, one for each column, with following keys:
			- cid: column ID
			- name: column name
			- type: column data type
			- notnull: whether or not the column can be NULL
			- dflt_value: default value
			- pk: whether or not column is primary key
		"""
		query = "PRAGMA table_info('%s')" % table_name
		cursor = self.get_cursor()
		cursor.execute(query)
		return [{key: row[key] for key in row.keys()} for row in cursor.fetchall()]

	def create_table(self,
		table_name,
		column_info_list):
		"""
		Create database table.

		:param table_name:
			str, table name
		:param column_info_list:
			list of column info specifications; these are either
			tuples (cid, name, type, notnull, dflt_value, pk)
			or dictionaries with these keys (as returned by
			:meth:`get_col_info`).
			The following data types are supported in sqlite:
			NULL, INTEGER, REAL, TEXT, DATE, TIMESTAMP, BLOB
			Note that only name is required, type defaults to NUMERIC,
			and cid is ignored.
		"""
		sql = 'CREATE TABLE %s(' % table_name
		for i, column_info in enumerate(column_info_list):
			if isinstance(column_info, (list, tuple)):
				cid, colname, coltype, notnull, dflt_value, primary_key = column_info
			else:
				cid = column_info.get('cid', 0)
				colname = column_info['name']
				coltype = column_info.get('type', 'NUMERIC')
				notnull = column_info.get('notnull', 0)
				dflt_value = column_info.get('dflt_value')
				primary_key = column_info.get('pk', 0)
			if i != 0:
				sql += ', '
			sql += '%s %s' % (colname, coltype)
			if dflt_value:
				sql += ' default %s' % dflt_value
			if notnull:
				sql += ' NOT NULL'
			if primary_key:
				sql += ' PRIMARY KEY'
		sql += ')'
		self.query_generic(sql)
		self.connection.commit()

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

	def add_column(self,
		table_name,
		name,
		type="NUMERIC",
		notnull=False,
		dflt_value=None,
		primary_key=False):
		"""
		Add column to database table.

		:param table_name:
			str, table name
		:param name:
			str, name of column
		:param type:
			str, column data type
			The following data types are supported in sqlite:
			NULL, INTEGER, REAL, TEXT, DATE, TIMESTAMP, BLOB
			(default: "NUMERIC")
		:param notnull:
			bool, whether or not column value is required not to be NULL
			(default: False)
		:param dflt_value:
			mixed, default value
			(default: None)
		:param primary_key:
			bool, whether or not column is a primary key
			(default: False)
		"""
		sql = 'ALTER TABLE %s ADD COLUMN %s %s'
		sql %= (table_name, name, type)
		if dflt_value:
			sql += ' default %s' % dflt_value
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
		:param name:
			str, name of column
		"""
		print("Warning: deleting a column is not supported by SQLite!")

	def add_records(self,
		table_name,
		recs,
		dry_run=False):
		"""
		Add records to database table.

		:param table_name:
			str, table name
		:param recs:
			list of dicts, mapping database table columns to values
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		cursor = self.get_cursor()
		for rec in recs:
			sql = "INSERT INTO %s (%s) VALUES (%s)"
			sql %= (table_name, ", ".join(rec.keys()), ', '.join(['?']*len(rec)))
			cursor.execute(sql, rec.values())

		if not dry_run:
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
		cursor = self.get_cursor()
		query = 'DELETE FROM %s' % table_name
		if where_clause.lstrip()[:5].upper() == "WHERE":
			where_clause = where_clause.lstrip()[5:]
		if where_clause:
			query += ' WHERE %s' % where_clause
		self.query_generic(query)

		if not dry_run:
			self.connection.commit()

	def update_records(self,
		table_name,
		col_dict,
		where_clause,
		dry_run=False):
		# Not sure if this should be kept
		if where_clause.lstrip()[:5].upper() == "WHERE":
			where_clause = where_clause.lstrip()[5:]

		cursor = self.get_cursor()
		for col_name, col_values in col_dict.items():
			query = 'UPDATE %s SET %s = ' % (table_name, col_name)
			#query += ', '.join(['%s = ?' % key for key in col_dict.keys()])
			query += ', '.join(['?'] * len(col_values))
			if where_clause:
				query += ' WHERE %s' % where_clause
			print(query[:1000])
			cursor.execute(query, col_values)

		if not dry_run:
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
		query = 'UPDATE %s SET ' % table_name
		query += ', '.join(['%s = ?' % key for key in col_dict.keys()])

		if where_clause.lstrip()[:5].upper() == "WHERE":
			where_clause = where_clause.lstrip()[5:]
		if where_clause:
			query += ' WHERE %s' % where_clause

		cursor.execute(query, col_dict.values())

		if not dry_run:
			self.connection.commit()

	def update_column(self,
		table_name,
		col_name,
		col_values,
		where_clause,
		order_clause='rowid',
		dry_run=False):
		"""
		Update values for a particular column in different records.

		:param table_name:
			string, name of database table
		:param col_name:
			string, name of column to update
		:param col_values:
			list of column values
		:param where_clause:
			string, where clause (REQUIRED !)
		:param order_clause:
			string, order clause
			(default: 'rowid')
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		## Query row IDs with where_clause
		row_ids = [rec['rowid'] for rec in self.query(table_name, 'rowid',
						where_clause=where_clause, order_clause=order_clause)]
		assert len(row_ids) == len(col_values)

		cursor = self.get_cursor()
		query = 'UPDATE %s SET %s=' % (table_name, col_name)

		## This does not give any error, but effectively writes all column values to each row!
		#query += '?'
		#col_data = [(val,) for val in col_values]

		## Use named parameters
		query += ':value WHERE rowid=:rowid'
		col_data = [dict(rowid=row_ids[i], value=col_values[i])
					for i in range(len(col_values))]
		cursor.executemany(query, col_data)

		if not dry_run:
			self.connection.commit()

	def vacuum(self,
		table_name=None):
		"""
		Clean empty records from database or database table.

		:param table_name:
			string, name of database table
			(default: None)
		"""
		query = "VACUUM"
		if table_name:
			query += " %s" % table_name
		self.query_generic(query)

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
			(default: Noe)
		"""
		if not idx_name:
			idx_name = "%s_IDX" % col_name
		query = "CREATE INDEX %s ON %s(%s)"
		query %= (idx_name, table_name, col_name)

	def print_schema(self):
		"""
		Print database schema.

		Source: http://stackoverflow.com/questions/11996394/is-there-a-way-to-get-a-schema-of-a-database-from-within-python
		"""
		for table_name in self.list_tables():
			print("{}:".format(table_name))
			for col_info in self.get_column_info(table_name):
				print("  {id}: {name}({type}){null}{default}{pk}".format(
					id=col_info['cid'],
					name=col_info['name'],
					type=col_info['type'],
					null=" not null" if col_info['notnull'] else "",
					default=" [{}]".format(col_info['dflt_value']) if col_info['dflt_value'] else "",
					pk=" *{}".format(col_info['pk']) if col_info['pk'] else "",
				))

	def get_sql_commands(self):
		"""
		Get list of SQL commands used to create the database.

		:return:
			list of strings, SQL commands
		"""
		query = 'SELECT * FROM sqlite_master'
		sql_commands = []
		for rec in self.query_generic(query):
			sql_commands.append(rec['sql'])
		return sql_commands

	def get_sqlite_version(self):
		"""
		Report SQLite version

		:return:
			string, SQLite version
		"""
		query = "SELECT sqlite_version()"
		return list(self.query_generic(query))[0].values()[0]

	def get_spatialite_version(self):
		"""
		Report SpatiaLite version

		:return:
			string, SpatiaLite version
		"""
		if self.has_spatialite:
			query = "SELECT spatialite_version()"
			return list(self.query_generic(query))[0].values()[0]

	def init_spatialite(self,
		populate_spatial_ref_sys="all"):
		"""
		Generate metadata table required by SpatiaLite

		:param populate_spatial_ref_sys:
			string, how to populate table 'spatial_ref_sys':
			- "all": will insert all 4000+ SRID definitions
			- "wgs84": will simply insert approx. 100 basic SRIDs based on
				WGS84 (WGS84 lat/long and WGS84/UTM), which will decrease
				the size of the database
			- "empty": will simply create the spatial_ref_sys table;
				no row will be inserted, this task will need to be handled
				manually at a later time
			(default: "all")
		"""
		if populate_spatial_ref_sys in ("empty", "wgs84"):
			query = "SELECT InitSpatialMetadata('%s')"
			query %= populate_spatial_ref_sys.upper()
		else:
			query = "SELECT InitSpatialMetadata()"

		return self.query_generic(query)

	def add_geometry_column(self,
		table_name,
		col_name="geom",
		geom_type="POINT",
		srid=4326,
		dim='XY',
		not_null=False):
		"""
		Add geometry column to spatialite database.

		:param table_name:
			string, name of database table
		:param col_name:
			string, name of column for geometry object
			(default: "geom")
		:param geom_type:
			string, one of 'POINT', 'LINESTRING', 'POLYGON', 'MULTIPOINT',
			'MULTILINESTRING', 'MULTIPOLYGON' or 'GEOMETRY' (generic
			geometry that may mix several geometries, note that this
			may not be supported by all GIS applications)
			(default: 'POINT')
		:param srid:
			int, spatial reference identifier
			(default: 4326 = WGS84)
		:param dim:
			string, one of 'XY', 'XYZ', 'XYM', 'XYZM'
			(default: 'XY')
		:param not_null:
			bool, whether or not geometry is NOT allowed to be null
			(default: False)
		"""
		# See: http://www.gaia-gis.it/spatialite-2.4.0-4/splite-python.html
		# and http://false.ekta.is/2011/04/pyspatialite-spatial-queries-in-python-built-on-sqlite3/
		query = "SELECT AddGeometryColumn('%s', '%s', %d, '%s', '%s', %d)"
		query %= (table_name, col_name, srid, geom_type, dim, not_null)
		return self.query_generic(query)

	def discard_geometry_column(self, table_name, geom_col="geom"):
		"""
		Remove any metadata and any trigger related to the given geometry
		column. This will leave any geometry-value stored within the
		corresponding table absolutely untouched.

		:param table_name:
			string, name of database table
		:param geom_col:
			string, name of geometry column
			(default: "geom")
		"""
		query = "SELECT DiscardGeometryColumn('%s', '%s')"
		query %= (table_name, geom_col)
		self.query_generic(query)
		# Commit required?

	def create_points_from_columns(self,
		table_name,
		x_col,
		y_col,
		z_col=None,
		geom_col="geom",
		where_clause="",
		srid=4326,
		dry_run=False):
		"""
		Create spatialite point objects for table records from x and y columns.

		:param table_name:
			string, name of database table
		:param x_col:
			string, name of column containing X coordinate
		:param y_col:
			string, name of column containing Y coordinate
		:param z_col:
			string, name of column containing Z coordinate
			Note that spatial queries do not take into account the Z
			coordinate. Note also that :meth:`add_geometry_column`
			must have been called with the appropriate :param:`dim`
			("XYZ" or "XYZM")
			(default: None)
		:param geom_col:
			string, name of geometry column
			(default: "geom")
		:param where_clause:
			string, where clause
			(default: "")
		:param srid:
			int, spatial reference identifier
			(default: 4326 = WGS84)
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		from collections import OrderedDict
		rowid_wkt_dict = OrderedDict()

		column_clause = [x_col, y_col, "rowid"]
		if z_col:
			column_clause.append(z_col)
		for rec in self.query(table_name, column_clause, where_clause=where_clause):
			if z_col:
				wkt = "POINTZ(%s %s %s)" % (rec[x_col], rec[y_col], rec[z_col])
			else:
				wkt = "POINT(%s %s)" % (rec[x_col], rec[y_col])
			rowid_wkt_dict[rec["rowid"]] = wkt
		self.set_geometry_from_wkt(table_name, rowid_wkt_dict, geom_col=geom_col,
									srid=srid, dry_run=dry_run)

	def set_geometry_from_wkt(self,
		table_name,
		rowid_wkt_dict,
		geom_col="geom",
		srid=4326,
		dry_run=False):
		"""
		Create spatialite geometric object for table records from WKT
		specification.

		:param table_name:
			string, name of database table
		:param rowid_wkt_dict:
			dict, mapping rowids to WKT strings
		:param geom_col:
			string, name of geometry column
			(default: "geom")
		:param srid:
			int, spatial reference identifier
			(default: 4326 = WGS84)
		:param dry_run:
			bool, whether or not to dry run the operation
			(default: False)
		"""
		cursor = self.get_cursor()
		query = "UPDATE %s SET %s=GeomFromText(?,%d) WHERE rowid=?"
		query %= (table_name, geom_col, srid)
		for rowid, wkt in rowid_wkt_dict.items():
			col_value = (wkt, rowid)
			cursor.execute(query, col_value)

		if not dry_run:
			self.connection.commit()

	def get_geometry_types(self,
		table_name,
		geom_col="geom"):
		"""
		Obtain geometry types for particular database table.

		:param table_name:
			string, name of database table
		:param geom_col:
			string, name of geometry column
			(default: "geom")

		:return:
			string, geometry type
		"""
		#query = "SELECT * from geometry_columns"
		query = "SELECT GeometryType(%s) FROM %s"
		query %= (geom_col, table_name)

		return set([rec.values()[0] for rec in list(self.query_generic(query))])

	def import_table_from_gis_file(self,
		gis_filespec,
		table_name=None,
		geom_col="geom"):
		"""
		Load GIS file in database table.

		:param gis_filespec:
			string, full path to GIS file
		:param table_name:
			string, name of database table
			(None, will use basename of :param:`gis_filespec`)
		:param geom_col:
			string, name of geometry column
			(default: "geom")
		"""
		from mapping.geo.readGIS import read_GIS_file, read_GIS_file_srs, wgs84

		## Determine srid
		srs = read_GIS_file_srs(gis_filespec)
		if srs.AutoIdentifyEPSG() == 0: # success
			srid = int(srs.GetAuthorityCode(None))
			out_srs = srs
		else:
			srid = 4326
			out_srs = wgs84

		## Create database table
		if not table_name:
			table_name = os.path.splitext(os.path.split(gis_filespec)[1])[0]

		gis_records = read_GIS_file(gis_filespec, out_srs=out_srs)
		col_names = gis_records[0].keys()
		col_names.remove('obj')
		col_names.remove('#')
		col_info_list = [dict(name=col_name) for col_name in col_names]
		self.create_table(table_name, col_info_list)

		## Write records
		geometries = []
		db_records = []
		for rec in gis_records:
			geom = rec.pop('obj')
			geometries.append(geom)
			rec.pop('#')
			db_records.append(rec)
		self.add_records(table_name, db_records)

		## Write geometries
		self.init_spatialite()
		geometry_types = set(obj.GetGeometryName() for obj in geometries)
		if len(geometries) > 1:
			geom_type = "GEOMETRY"
		else:
			geom_type = geometries[0]
		self.add_geometry_column(table_name, geom_col, geom_type, srid=srid)
		rowid_wkt_dict = {rowid+1: geom.ExportToWkt() for rowid, geom in
							enumerate(geometries)}
		self.set_geometry_from_wkt(table_name, rowid_wkt_dict, geom_col, srid=srid)

	def compress_geometry(self,
		table_name,
		geom_col="geom"):
		"""
		Reduce space taken by geometry info

		:param table_name:
			string, name of database table
		:param geom_col:
			string, name of geometry column
			(default: "geom")
		"""
		query = "UPDATE %s SET %s = CompressGeometry(%s)"
		query %= (table_name, geom_col, geom_col)
		self.query_generic(query)
		self.connection.commit()

	#TODO: how to create spatial index??



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
		print(query)
	cur.execute(query)
	return cur.fetchall()


def query_sqlite_db(
	db_filespec,
	table_clause,
	column_clause="*",
	join_clause="",
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

	:return:
		generator object, yielding a dictionary for each record
	"""
	query = build_sql_query(table_clause, column_clause, join_clause,
							where_clause, having_clause, order_clause)
	return query_sqlite_db_generic(db_filespec, query, verbose=verbose)





if __name__ == "__main__":
	db_filespec = u"C:\\Users\\kris\\ownCloud\\Mendeley Desktop\\kris.vanneste@oma.be@www.mendeley.com.sqlite"
	if os.path.exists(db_filespec):
		sqldb = SQLiteDB(db_filespec)
		print(sqldb.list_tables())
		for rec in sqldb.query("Documents", verbose=True):
			pass
		print(rec.items())
