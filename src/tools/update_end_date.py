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

import xarray as xr
import datetime
import os

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations

def update_end_date(dataset_id=None):
    store = Store()

    processed = 0
    with SchemaOperations(store) as ops:
        datasets = ops.list_datasets()
    for dataset in datasets:
        if dataset_id is None or dataset.dataset_id == dataset_id:
            print("Obtaining End Date for Dataset=%s"%dataset.dataset_id)
            location = dataset.location

            year_marker = location.find("{YEAR}")
            if year_marker >= 0:
                parent_folder = location[:year_marker]
                years = os.listdir(parent_folder)
                last_year = sorted(years)[-1]
                location = location.replace("{YEAR}",last_year)

            # this can take some time
            print("Obtaining End Date for Dataset=%s from %s" % (dataset.dataset_id, location))
            ds = xr.open_mfdataset(location)

            last_ts = ds["time"].values.tolist()[-1]

            end_dt = datetime.datetime.fromtimestamp(last_ts / 1e9).date()
            ds.close()

            print("Set End Date for Dataset=%s: %s" % (dataset.dataset_id, Store.encode_date(end_dt)))
            with SchemaOperations(store) as ops:
                ops.update_dataset_end_date(dataset.dataset_id, end_dt)
            processed += 1


    if dataset_id is not None and processed == 0:
        raise Exception(f"Unable to find dataset with id {dataset_id}")




if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id",default=None)

    args = parser.parse_args()

    update_end_date(args.dataset_id)