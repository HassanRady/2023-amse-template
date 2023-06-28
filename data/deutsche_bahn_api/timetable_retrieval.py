from __future__ import annotations

import os
import sys
sys.path.append(os.path.abspath(__file__))

from logger import get_file_logger
_logger = get_file_logger(__name__, 'debug')

import xml.etree.ElementTree as elementTree

from data.deutsche_bahn_api.train_plan import TrainPlan
from data.deutsche_bahn_api.plan_change import PlanChange

class TimeTableHandler:
    def __init__(self) -> None:
        pass

    def get_timetable_data(self, api_response) -> list[TrainPlan]:
        train_list = []
        trains = elementTree.fromstringlist(api_response.text)
        for train in trains:
            for train_details in train:
                if train_details.tag == "tl":
                    trip_label_object = train_details.attrib
                if train_details.tag == "dp":
                    departure_object = train_details.attrib
                else:
                    departure_object = None
                if train_details.tag == "ar":
                    arrival_object = train_details.attrib
                else:
                    arrival_object = None

            if not departure_object:
                """ Arrival without departure """
                continue

            train_object = TrainPlan()
            train_object.EVA_NR = api_response.EVA_NR
            train_object.stop_id = train.attrib["id"]
            train_object.train_type = trip_label_object["c"]
            train_object.train_number = trip_label_object["n"]
            train_object.platform = departure_object['pp']
            train_object.next_stations = departure_object['ppth']
            train_object.departure = departure_object['pt']

            if "f" in trip_label_object:
                train_object.trip_type = trip_label_object["f"]
            else:
                train_object.trip_type = "N/A"

            if "l" in departure_object:
                train_object.train_line = departure_object['l']
            else:
                train_object.train_line = "N/A"

            if arrival_object:
                train_object.passed_stations = arrival_object['ppth']
                train_object.arrival = arrival_object['pt']
            else:
                train_object.arrival = "N/A"
                train_object.passed_stations = "N/A"

            train_list.append(train_object)

        return train_list

    def get_timetable_changes_data(self, api_response) -> list[TrainPlan]:
        changed_trains = elementTree.fromstringlist(api_response.text)

        plans_change = []

        for changed_train in changed_trains:
            plan_change = PlanChange()
            plan_change.messages = []

            if "eva" in changed_train.attrib:
                plan_change.EVA_NR = int(changed_train.attrib["eva"])
            else:
                continue

            plan_change.stop_id = changed_train.attrib["id"]

            for changes in changed_train:
                if changes.tag == "dp":
                    if "ct" in changes.attrib:
                        plan_change.departure = changes.attrib["ct"]
                    else:
                        plan_change.departure = "N/A"
                    if "cpth" in changes.attrib:
                        plan_change.next_stations = changes.attrib["cpth"]
                    else:
                        plan_change.next_stations = "N/A"
                    if "cp" in changes.attrib:
                        plan_change.platform = changes.attrib["cp"]
                    else:
                        plan_change.platform = "N/A"

                if changes.tag == "ar":                    
                    if "ct" in changes.attrib:
                        plan_change.arrival = changes.attrib["ct"]
                    else:
                        plan_change.arrival = "N/A"                       
                    if "cpth" in changes.attrib:
                        plan_change.passed_stations = changes.attrib["cpth"]
                    else:
                        plan_change.passed_stations = "N/A"
                    
            plans_change.append(plan_change)

        return plans_change
