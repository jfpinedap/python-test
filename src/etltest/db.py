"SQLite manager"

# Base imports
import ast
import sqlite3

# Data base credentials
DATABASE = 'database.db3'
SCHEMA = 'src/etltest/schema.sql'


def get_engine():
  engine = sqlite3.connect(DATABASE)

  with open(SCHEMA, "r", encoding="utf-8") as f:
    engine.executescript(f.read())

  return engine
