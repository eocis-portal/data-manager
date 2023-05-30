# -*- coding: utf-8 -*-

#    EOCIS data-manager
#    Copyright (C) 2020-2023  National Centre for Earth Observation (NCEO)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import json
import yaml
import datetime
from yaml import Loader

# date format for parsing data values from the YAML file
DATE_FORMAT = "%d-%m-%Y"

def parse_date(s):
    return datetime.datetime.strptime(s,DATE_FORMAT).date()

"""
The dataset module deals with the representation of a dataset, consisting of one or more variables.
"""

class Variable:

    def __init__(self,variable_id:str,variable_name:str,spec:dict):
        self.variable_id = variable_id
        self.variable_name = variable_name
        self.spec = spec

    def __repr__(self) -> str:
        spec = json.dumps(self.spec)
        return f"Variable({self.variable_id},{self.variable_name},{spec})"

    def __eq__(self, other:"Variable") -> bool:
        return self.variable_id == other.variable_id \
            and self.variable_name == other.variable_name \
            and self.spec == other.spec


class DataSet:

    VALID_TEMPORAL_RESOLUTIONS = ["daily","pentad","dekad","monthly","yearly"]
    VALID_SPATIAL_RESOLUTIONS = ["0.05","0.1","0.25","0.5","1"]

    def __init__(self, dataset_id:str, dataset_name:str, temporal_resolution:str, spatial_resolution:str, start_date:datetime.date, end_date:datetime.date, location:str, spec:dict, variables:list[Variable], enabled:bool=True):
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        if temporal_resolution not in DataSet.VALID_TEMPORAL_RESOLUTIONS:
            raise ValueError(temporal_resolution)
        self.temporal_resolution = temporal_resolution
        if spatial_resolution not in DataSet.VALID_SPATIAL_RESOLUTIONS:
            raise ValueError(spatial_resolution)
        self.spatial_resolution = spatial_resolution
        self.start_date = start_date
        self.end_date = end_date
        self.location = location
        self.spec = spec
        self.variables = variables
        self.enabled = enabled

    @staticmethod
    def load_dataset_from_file(path) -> "DataSet":
        filename = os.path.split(path)[1]
        dataset_id = os.path.splitext(filename)[0]
        with open(path) as f:
            dataset_obj = yaml.load(f.read(),Loader=Loader)
            enabled = dataset_obj.get("enabled",True)
            dataset_name = dataset_obj["name"]
            temporal_resolution = dataset_obj["temporal_resolution"]
            spatial_resolution = dataset_obj["spatial_resolution"]
            start_date = parse_date(dataset_obj["start_date"])
            location = dataset_obj["location"]
            dataset_spec = dataset_obj.get("spec",{})
            variable_list = dataset_obj.get("variables",{})
            variables = []
            for (id,variable) in variable_list.items():
                name = variable["name"]
                variable_spec = variable.get("spec",{})
                variables.append(Variable(id,name,variable_spec))

            return DataSet(dataset_id, dataset_name=dataset_name,
                           temporal_resolution=temporal_resolution,
                           spatial_resolution=spatial_resolution,
                           start_date=start_date,
                           end_date=None,
                           location=location,
                           spec=dataset_spec,
                           variables=variables,
                           enabled=enabled)

    @staticmethod
    def load_datasets(folder) -> list["DataSet"]:
        datasets = []
        for filename in os.listdir(folder):
            if filename.endswith(".yaml"):
                path = os.path.join(folder, filename)
                dataset = DataSet.load_dataset_from_file(path)
                datasets.append(dataset)
        return datasets

    def __repr__(self) -> str:
        spec = json.dumps(self.spec)
        variables = ", ".join([str(v) for v in self.variables])
        return f"DataSet({self.dataset_id},{self.dataset_name},{self.temporal_resolution},{self.spatial_resolution},{self.start_date},{self.end_date},{self.location},{spec},{variables})"

    def __eq__(self,other) -> bool:
        return self.dataset_id == other.dataset_id \
            and self.dataset_name == other.dataset_name \
            and self.temporal_resolution == other.temporal_resolution \
            and self.spatial_resolution == other.spatial_resolution \
            and self.start_date == other.start_date \
            and self.end_date == other.end_date \
            and self.spec == other.spec \
            and self.variables == other.variables
