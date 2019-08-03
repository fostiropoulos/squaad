"""
Lightweight sqlalchemy class for quick reads and writes to psql tables

"""

import datetime
from dataclasses import dataclass, field, asdict, astuple, fields
from typing import *
import sqlalchemy
import pandas as pd
from sqlalchemy.types import String, Float, DateTime, BigInteger


"""
Example usage:

# @dataclass
# class TableExample(BaseConverters):
# 	col1: type
# 	col2: type

sql -> dict repr
sql_table -> tableexample, ((col1, type), (col2, type))
cols -> [col1, col2]
name -> tableexample

"""

pytype_to_sqltype = {str: String, datetime.datetime: DateTime, float: Float, int: BigInteger}


@dataclass
class BaseConverters:

	def sql(self):
		return asdict(self)

	@classmethod
	def sql_table(cls):
		cols = tuple((f.name, pytype_to_sqltype[f.type]) for f in fields(cls))
		name = cls.__name__.lower()
		return name, cols

	@classmethod
	def map_pd_cols(cls, cols):
		return dict((x, y) for x, y in zip(cols, [f.name for f in fields(cls)]))

	@classmethod
	def cols(cls):
		return [f.name for f in fields(cls)]

	@classmethod
	def name(cls):
		return cls.__name__.lower()


def connect_sqlalchemy_db(db_name, user):
	connection_string = "postgresql+psycopg2://{}@localhost:5432/{}".format(user, db_name)

	engine = sqlalchemy.create_engine(connection_string).connect()
	meta = sqlalchemy.MetaData(engine)
	return engine, meta


class db_lite(object):

	def __init__(self, db_name: str, user: str):
		self.engine, self.meta = connect_sqlalchemy_db(db_name, user)
		self.meta.reflect()

	def add_table(self, class_, replace=True):

		name, col_defs = class_.sql_table()
		columns = (sqlalchemy.Column(*x) for x in col_defs)

		if name in self.meta.tables:
			if replace:
				self.drop_table(class_)
			else:
				return

		t = sqlalchemy.Table(name, self.meta, *columns)
		t.create(self.engine)

	def drop_table(self, class_):

		name = class_.name()
		sqlalchemy.Table(name, self.meta).drop(self.engine)
		self.meta.remove(sqlalchemy.Table(name, self.meta))
		self.meta.reflect()

	def add_row(self, obj):

		name = obj.name()

		stmnt = self.meta.tables[name].insert().values(obj.sql())
		self.engine.execute(stmnt)

	def update_row(self, name, cond_col, cond_val, vals):
		t = self.meta.tables[name]
		stmnt = sqlalchemy.update(t).where(t.c[cond_col] == cond_val).values(vals)
		self.engine.execute(stmnt)

	def add_rows(self, objs):
		name = objs[0].name()

		self.engine.execute(self.meta.tables[name].insert(), [obj.sql() for obj in objs])

	def pd_to_table(self, df, name, if_exists='replace'):
		df.to_sql(name, con=self.engine, if_exists=if_exists, index=False)
		self.meta.reflect()

	def table_to_pd(self, name, cols=None, index_col=None, parse_dates=None):
		return pd.read_sql_table(name, con=self.engine, columns=cols, index_col=index_col, parse_dates=parse_dates)
