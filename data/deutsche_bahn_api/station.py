from typing import NamedTuple
from data.config import config


class Station(NamedTuple):
    EVA_NR: int
    DS100: str
    IFOPT: str
    NAME: str
    Verkehr: str
    Laenge: str
    Breite: str
    Betreiber_Name: str
    Betreiber_Nr: int
    Status: str

    def insert_to_db(self, db_engine):
        db_engine.execute(
            f"""
            INSERT OR REPLACE INTO {config.database.STATION_TABLE} VALUES ({self.EVA_NR}, '{self.DS100}', '{self.IFOPT}', '{self.NAME}',
            '{self.Verkehr}', '{self.Laenge}', '{self.Breite}', '{self.Betreiber_Name}', {self.Betreiber_Nr}, '{self.Status}');
           """

        )
