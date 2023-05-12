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
import yaml
from yaml import Loader

"""
The bundle module deals with the representation of a logical bundle of data, consisting of one or more datasets.
"""


class Bundle:

    def __init__(self, bundle_id:str, bundle_name:str, spec:dict, dataset_ids:list[str], minx:float, miny:float, maxx:float, maxy:float):
        self.bundle_id = bundle_id
        self.bundle_name = bundle_name
        self.spec = spec
        self.dataset_ids = dataset_ids
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy

    @staticmethod
    def load_bundle_from_file(path:str) -> "Bundle":
        filename = os.path.split(path)[1]
        bundle_id = os.path.splitext(filename)[0]
        with open(path) as f:
            bundle_obj = yaml.load(f.read(),Loader=Loader)
            bundle_name = bundle_obj["name"]
            bundle_spec = bundle_obj.get("spec",{})
            dataset_ids = bundle_obj.get("datasets",[])
            minx = bundle_obj.get("minx")
            miny = bundle_obj.get("miny")
            maxx = bundle_obj.get("maxx")
            maxy = bundle_obj.get("maxy")
            return Bundle(bundle_id, bundle_name=bundle_name, spec=bundle_spec,
                          dataset_ids=dataset_ids, minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    @staticmethod
    def load_bundles(folder) -> list["Bundle"]:
        bundles = []
        for filename in os.listdir(folder):
            if filename.endswith(".yaml"):
                path = os.path.join(folder,filename)
                bundles.append(Bundle.load_bundle_from_file(path))
        return bundles

    def __repr__(self) -> str:
        import json
        spec = json.dumps(self.spec)
        dataset_ids = json.dumps(self.dataset_ids)
        return f"Bundle({self.bundle_id},{self.bundle_name},{spec},{dataset_ids}," \
               f"{self.minx},{self.miny},{self.maxx},{self.maxy})"

    def __eq__(self,other) -> bool:
        return self.bundle_id == other.bundle_id \
            and self.bundle_name == other.bundle_name \
            and sorted(self.dataset_ids) == sorted(other.dataset_ids) \
            and self.spec == other.spec \
            and self.minx == other.minx \
            and self.maxx == other.maxx \
            and self.miny == other.miny \
            and self.maxy == other.maxy


