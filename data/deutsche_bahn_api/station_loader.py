import json
import pkgutil
import mpu

from deutsche_bahn_api.station import Station


def normalize_lat_or_long_from_station(station: Station) -> dict[str, float]:
    lat = station.Breite.replace(",", ".")
    long = station.Laenge.replace(",", ".")

    return {
        'lat': float(lat),
        'long': float(long)
    }


class StationLoader:
    def __init__(self) -> None:
        self.stations_list = []

    def load_stations(self):
        json_raw = pkgutil.get_data(__name__, "static/train_stations_list.json")
        stations = json.loads(json_raw)
        for item in stations:
            self.stations_list.append(Station(**item))

    def find_stations_by_lat_long(self, target_lat: float, target_long: float, radius: int) -> list[Station]:
        results: list[Station] = []

        for station in self.stations_list:
            lat_long: dict[str, float] = normalize_lat_or_long_from_station(station)

            distance = mpu.haversine_distance(
                (lat_long['lat'], lat_long['long']),
                (target_lat, target_long))
            if distance < radius:
                results.append(station)

        return results

    def find_stations_by_name(self, query: str) -> list[Station]:
        results: list[Station] = []

        for station in self.stations_list:
            if query in station.NAME:
                results.append(station)

        return results

