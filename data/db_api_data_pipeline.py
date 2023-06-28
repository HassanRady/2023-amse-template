import sys
import os
sys.path.append(os.path.abspath(''))

from logger import get_file_logger
_logger = get_file_logger(__name__, 'debug')

from database_client import SqliteClient
from deutsche_bahn_api.api_caller import ApiClient
from deutsche_bahn_api.timetable_retrieval import TimeTableHandler
from deutsche_bahn_api.station_loader import StationLoader


api_client = ApiClient()
station_helper = StationLoader()
station_helper.load_stations()
timetable_handler = TimeTableHandler()

sample = sample=station_helper.stations_list[100:110]

def start_full_pipeline():
    for station in station_helper.stations_list:
        station.insert_to_db(SqliteClient.db_engine)
    start_api_pipeline()

def start_api_pipeline():
    for station in sample:
        print(f"Processing: {station.EVA_NR}")

        response = api_client.get_current_hour_station_timetable(
            station.EVA_NR)

        if "Request Rejected" in response.text:
            print(f"Bad station data {station.EVA_NR}")
            continue
            
        if response.status_code != 200:
            continue

        trains_in_this_hour = timetable_handler.get_timetable_data(
            response)
        
        for train_plan in trains_in_this_hour:
            if train_plan.arrival == "N/A" or train_plan.departure == "N/A":
                continue
            train_plan.insert_into_db(SqliteClient.db_engine)

    print("-"*20)

    for station in sample:
        print(f"Processing: {station.EVA_NR}")

        response = api_client.get_all_timetable_changes_from_station(
            station.EVA_NR)
        
        if "Request Rejected" in response.text:
            print(f"Bad station data {station.EVA_NR}")
            continue

        if response.status_code != 200:
            continue

        plans_change = timetable_handler.get_timetable_changes_data(response)

        for plan_change in plans_change:
            if plan_change.arrival == "N/A" or plan_change.departure == "N/A":
                continue
            plan_change.insert_into_db(SqliteClient.db_engine)

    SqliteClient.db_engine.close()

if __name__ == "__main__":
    start_full_pipeline()
