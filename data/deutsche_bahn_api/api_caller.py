if __name__ == "__main__":
    import sys
    sys.stderr.write('\x1b[0;30;41m' + 'CAN NOT BE RUN DIRECTLY, MUST RUN FROM (data) DIRECTORY' + '\x1b[0m' +"\n")

import os

from logger import get_file_logger
_logger = get_file_logger(__name__, 'debug')

import requests
from datetime import datetime

class ApiClient:
    def __init__(self,) -> None:
        self.client_id = os.environ["DB_CLIENT_ID"]
        self.client_secret = os.environ["DB_API_KEY"]

        self.headers = {
                "DB-Api-Key": self.client_secret,
                "DB-Client-Id": self.client_id,
            }

    def get_hour_station_timetable(self, station_number, hour):
        hour_date = datetime.strptime(str(hour), "%H")
        hour = hour_date.strftime("%H")
        current_date = datetime.now().strftime("%y%m%d")
        
        response = requests.get(
            f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
            f"/plan/{station_number}/{current_date}/{hour}",
            headers=self.headers
        )
        return response

    def get_current_hour_station_timetable(self, station_number):
        current_hour = datetime.now().strftime("%H")
        current_date = datetime.now().strftime("%y%m%d")

        response = requests.get(
            f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
            f"/plan/{station_number}/{current_date}/{current_hour}",
            headers=self.headers
        )
        response.EVA_NR = station_number
        return response

    def get_all_timetable_changes_from_station(self, station_number: int) -> str:
        response = requests.get(
            f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{station_number}",
            headers=self.headers
        )
        return response
    
    def __str__(self) -> str:
        return "ApiClient(DB_CLIENT_ID=***, DB_API_KEY=***)"
    


