import pandas as pd
from weather_data.data_getter import get_station_description, read_weather_files, extract_zip_file, get_links_from_page, get_link_content
from database_client import SqliteClient
from deutsche_bahn_api.api_caller import ApiClient
from deutsche_bahn_api.station_loader import StationLoader
from deutsche_bahn_api.timetable_retrieval import TimeTableHandler
from unittest import TestCase
from config import config
import sys
import os
sys.path.append(os.path.abspath('') + '/data')


api_client = ApiClient()
station_loader = StationLoader()
station_loader.load_stations()
sample_station = station_loader.stations_list[10].EVA_NR

SqliteClient.db_engine.execute("""
    CREATE TABLE IF NOT EXISTS stations (
EVA_NR int,
DS100 text,
IFOPT text,
NAME text,
Verkehr text,
Laenge text,
Breite text,
Betreiber_Name text,
Betreiber_Nr int,
Status text,
PRIMARY KEY (EVA_NR)
);
""")
SqliteClient.db_engine.execute("""
    
CREATE TABLE IF NOT EXISTS train_plan (
EVA_NR int,
stop_id text,
trip_type text,
train_type text,
train_number text,
train_line text,
platform text,
next_stations text,
passed_stations text,
arrival text,
departure text,
FOREIGN KEY (EVA_NR) REFERENCES stations(EVA_NR),
    PRIMARY KEY (stop_id)
);
""")
SqliteClient.db_engine.execute("""

CREATE TABLE IF NOT EXISTS plan_change(
EVA_NR int,
stop_id text,
next_stations text,
passed_stations text,
arrival text,
departure text,
platform text,
FOREIGN KEY (EVA_NR) REFERENCES stations(EVA_NR),
FOREIGN KEY (stop_id) REFERENCES train_plan(stop_id),
PRIMARY KEY (EVA_NR, stop_id)
);
""")


class TestDataPipeline(TestCase):

    def test_pipeline(self):

        self.station_loader = StationLoader()
        self.api_client = ApiClient()
        self.timetable_handler = TimeTableHandler()
        self.sqlite_client = SqliteClient()
        self.station_loader.load_stations()
        self.sample_station = self.station_loader.stations_list[10].EVA_NR
        response = self.api_client.get_current_hour_station_timetable(
            sample_station)
        trains_this_hour = self.timetable_handler.get_timetable_data(response)
        response = self.api_client.get_all_timetable_changes_from_station(
            sample_station)
        plan_changes = self.timetable_handler.get_timetable_changes_data(
            response)

        sample_plan_change = plan_changes[0]
        sample_train = trains_this_hour[0]
        sample_train.insert_into_db(
            SqliteClient.db_engine,)
        sample_plan_change.insert_into_db(
            SqliteClient.db_engine,)

        df = pd.read_sql(
            f"select * from {config.database.TRAIN_TABLE}", SqliteClient.db_engine)
        df = df.query(f"EVA_NR == {sample_train.EVA_NR}")
        self.assertGreater(len(df), 0)

        df = pd.read_sql(
            f"select * from {config.database.PLAN_CHANGE_TABLE}", SqliteClient.db_engine)
        df = df.query(f"EVA_NR == {sample_plan_change.EVA_NR}")
        self.assertGreater(len(df), 0)


class WeatherDataTest(TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def test_station_loader(self):
        name = "KL_Tageswerte_Beschreibung_Stationen.txt"
        get_station_description(name)
        df = pd.read_sql(
            "select * from weather_station_description", SqliteClient.db_engine)
        expected = ['Stations_id', 'von_datum', 'bis_datum', 'Stationshoehe', 'geoBreite',
                    'geoLaenge', 'Stationsname', 'Bundesland']
        actual = df.columns
        assert (actual == expected).all()

    def test_data_to_database(self):
        links = get_links_from_page(config.weather_data.WEATHER_DATA_URL)

        target_links = []
        sample = 1
        for link_name in links[:sample]:
            if ".zip" in link_name:
                target_links.append(link_name)
        for zip_link in target_links:
            content = get_link_content(zip_link)
            extract_zip_file(content)

        df = read_weather_files(config.weather_data.WEATHER_DATA_PATH)

        df.to_sql("raw_weather_data", SqliteClient.db_engine,
                  index=False, if_exists='replace')

        test_df = pd.read_sql(
            "select * from raw_weather_data", SqliteClient.db_engine)
        expected = ['STATIONS_ID', 'MESS_DATUM', 'QN_3', '  FX', '  FM', 'QN_4', ' RSK',
                    'RSKF', ' SDK', 'SHK_TAG', '  NM', ' VPM', '  PM', ' TMK', ' UPM',
                    ' TXK', ' TNK', ' TGK', 'eor']

        assert (test_df.columns == expected).all()


class TrainStationLoaderTest(TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.station_loader = StationLoader()

    def _test_station_loader(self):
        self.station_loader.load_stations()
        expected = "Station(EVA_NR=8002551, DS100='AELB', IFOPT='de:02000:11943', NAME='Hamburg Elbbrücken', Verkehr='RV', Laenge='10,0245', Breite='53,5345', Betreiber_Name='DB Station und Service AG', Betreiber_Nr=0, Status='neu')"
        actual = self.station_loader.stations_list[0]
        self.assertEqaual(actual, expected)

    def test_stations_number(self):
        self.station_loader.load_stations()
        expected = 6519
        assert len(self.station_loader.stations_list) == expected


class ApiCallTest(TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.api_client = ApiClient()
        station_helper = StationLoader()
        station_helper.load_stations()
        self.sample_station = station_helper.stations_list[10].EVA_NR

    def test_timetable_station_call_success_status_code(self):
        response = self.api_client.get_current_hour_station_timetable(
            self.sample_station)
        expected = 200
        assert response.status_code == expected

    def test_plan_change_station_call_success_status_code(self):
        response = self.api_client.get_all_timetable_changes_from_station(
            self.sample_station)
        expected = 200
        assert response.status_code == expected


class TimeTableDataHandlerTest(TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.sample_station = station_loader.stations_list[10].EVA_NR
        self.timetable_handler = TimeTableHandler()

    def test_train_plan_data_existence(self):
        response = api_client.get_current_hour_station_timetable(
            self.sample_station)
        trains_this_hour = self.timetable_handler.get_timetable_data(response)
        sample_train = trains_this_hour[0]

        assert sample_train.EVA_NR == self.sample_station
        assert sample_train.stop_id is not None
        assert sample_train.arrival is not None
        assert sample_train.departure is not None
        assert sample_train.next_stations is not None

    def test_plan_change_data_existence(self):
        response = api_client.get_all_timetable_changes_from_station(
            self.sample_station)
        plan_changes = self.timetable_handler.get_timetable_changes_data(
            response)
        sample_plan_change = plan_changes[0]

        assert sample_plan_change.EVA_NR == self.sample_station
        assert sample_plan_change.stop_id is not None
        assert sample_plan_change.arrival is not None
        assert sample_plan_change.departure is not None
        assert sample_plan_change.next_stations is not None


class SqliteInsertionTest(TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.timetable_handler = TimeTableHandler()

    def test_train_plan_insertion(self):
        response = api_client.get_current_hour_station_timetable(
            sample_station)
        trains_this_hour = self.timetable_handler.get_timetable_data(response)
        sample_train = trains_this_hour[0]

        sample_train.insert_into_db(
            SqliteClient.db_engine)

        df = pd.read_sql(
            f"select * from {config.database.TRAIN_TABLE}", SqliteClient.db_engine)
        df = df.query(f"EVA_NR == {sample_train.EVA_NR}")

        assert len(df) > 0

    def test_plan_change_insertion(self):
        response = api_client.get_all_timetable_changes_from_station(
            sample_station)
        plan_changes = self.timetable_handler.get_timetable_changes_data(
            response)
        sample_plan_change = plan_changes[0]

        sample_plan_change.insert_into_db(
            SqliteClient.db_engine)

        df = pd.read_sql(
            f"select * from {config.database.PLAN_CHANGE_TABLE}", SqliteClient.db_engine)
        df = df.query(f"EVA_NR == {sample_plan_change.EVA_NR}")

        assert len(df) > 0
