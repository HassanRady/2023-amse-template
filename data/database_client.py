import sys
import os
sys.path.append(os.path.abspath(''))

import sqlite3
import pandas as pd

from config import config


class SqliteClient:
    try:
        db_engine = sqlite3.connect(config.database.DATABASE_NAME)
    except sqlite3.Error as error:
        sys.stderr.write('\x1b[0;30;41m' + "Error while connecting to sqlite" + '\x1b[0m' +"\n", error)

    @staticmethod
    def get_data_from_table(table_name: str):
        return pd.read_sql_query(f"select * from {table_name};", SqliteClient.db_engine)