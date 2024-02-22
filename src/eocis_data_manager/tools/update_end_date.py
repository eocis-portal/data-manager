# -*- coding: utf-8 -*-
import glob

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
import calendar

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations


def update_end_date(dataset_id=None):
    store = Store()

    processed = 0
    with SchemaOperations(store) as ops:
        datasets = ops.list_datasets()
    for dataset in datasets:

        current_year = datetime.datetime.now().year

        if dataset_id is None or dataset.dataset_id == dataset_id:
            print("Obtaining End Date for Dataset=%s"%dataset.dataset_id)
            year = current_year
            found = False
            while year >= dataset.start_date.year and not found:
                location = dataset.location.replace("{YEAR}",str(year)).replace("{MONTH}","*").replace("{DAY}","*")
                print(location)
                if len(glob.glob(location)) > 0:
                    # this can take some time
                    print("Obtaining End Date for Dataset=%s from %s" % (dataset.dataset_id, location))
                    ds = xr.open_mfdataset(location,concat_dim="time",combine="nested",combine_attrs="drop_conflicts",data_vars=[dataset.variables[0].variable_id])

                    last_ts = ds["time"].values.tolist()[-1]

                    end_dt = datetime.datetime.fromtimestamp(last_ts / 1e9).date()
                    if dataset.get_temporal_resolution() == "monthly":
                        # some datasets set the timestamp to be the start or the middle of the month, not the end
                        # round up the end date to the end of the month
                        (_,days_in_month) = calendar.monthrange(end_dt.year,end_dt.month)
                        end_dt = datetime.date(end_dt.year,end_dt.month, days_in_month)
                    ds.close()

                    print("Set End Date for Dataset=%s: %s" % (dataset.dataset_id, Store.encode_date(end_dt)))
                    with SchemaOperations(store) as ops:
                        ops.update_dataset_end_date(dataset.dataset_id, end_dt)
                    processed += 1
                    found = True
                else:
                    year -= 1
            if not found:
                print("Failed to obtain End Date for Dataset=%s" % dataset.dataset_id)


    if dataset_id is not None and processed == 0:
        raise Exception(f"Unable to find dataset with id {dataset_id}")




if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id",default=None)

    args = parser.parse_args()

    update_end_date(args.dataset_id)