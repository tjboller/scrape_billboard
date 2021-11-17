import sqlite3
from typing import Dict
import os

import pandas as pd

from utils import logging
from utils.config import __config__

LOGGER = logging.get_logger(__name__)
CURR_DIR = os.path.dirname(os.path.abspath(__file__))


class DbConnection:
    def __init__(self):
        self.connection = sqlite3.connect(
            os.path.join(CURR_DIR, __config__['db_name'])
        )

    def read(self, query, parameters=None):
        cur = self.connection.cursor()
        try:
            parameters = parameters if parameters else {}
            cur.execute(query, parameters)
            return cur.fetchall()
        finally:
            cur.close()

    def read_to_pandas(self, query: str, parameters: Dict=None) -> pd.DataFrame:
        return pd.read_sql(query, con=self.connection, params=parameters)

    def df_dump(self, df: pd.DataFrame, table_name: str, if_exists: str='replace', **kwargs):
        df.to_sql(
            name=table_name,
            con=self.connection,
            index=False,
            if_exists=if_exists,
            **kwargs)
