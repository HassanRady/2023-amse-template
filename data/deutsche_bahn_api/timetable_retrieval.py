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
            train_data = {}
            for train_details in train:
                if train_details.tag == "tl":
                    # train_data['train_label'] = train_details.attrib
                    train_data['train_label'] = train_details.attrib
                if train_details.tag == "dp":
                    # train_data['departure'] = train_details.attrib
                    train_data['departure'] = train_details.attrib
                if train_details.tag == "ar":
                    # train_data['arrival'] = train_details.attrib
                    train_data['arrival'] = train_details.attrib

            if train_data.get('departure') is None:
                """ Arrival without departure """
                continue

            train_object = TrainPlan()
            train_object.EVA_NR = api_response.EVA_NR
            train_object.stop_id = train.attrib["id"]
            train_object.train_type = train_data['train_label']["c"] if train_data.get('train_label') is not None else "N/A"
            train_object.train_number = train_data['train_label']["n"]
            train_object.platform = train_data['departure']['pp']
            train_object.next_stations = train_data['departure']['ppth']
            train_object.departure = train_data['departure']['pt']

            if "f" in train_data['train_label']:
                train_object.trip_type = train_data['train_label']["f"]
            else:
                train_object.trip_type = "N/A"

            if "l" in train_data['departure']:
                train_object.train_line = train_data['departure']['l']
            else:
                train_object.train_line = "N/A"

            if train_data.get('arrival') is not None:
                train_object.passed_stations = train_data['arrival']['ppth']
                train_object.arrival = train_data['arrival']['pt']
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
                change_data = {}
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
                    if "ct" in changes.attrib and changes.attrib["ct"] is not None:
                        plan_change.arrival = changes.attrib["ct"]
                    else:
                        plan_change.arrival = "N/A"                       
                    if "cpth" in changes.attrib and changes.attrib['cpth'] is not None:
                        plan_change.passed_stations = changes.attrib["cpth"]
                    else:
                        plan_change.passed_stations = "N/A"
                    
            plans_change.append(plan_change)

        return plans_change
