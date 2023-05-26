
import xarray as xr
import datetime
import os

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations

def update_end_date(dataset_id=None):
    store = Store()

    processed = 0
    with SchemaOperations(store) as ops:
        datasets = ops.listDataSets()
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

                print("Set End Date for Dataset=%s: %s" % (dataset.dataset_id, Store.encodeDate(end_dt)))
                with SchemaOperations(store) as ops:
                    ops.updateEndDate(dataset_id, end_dt)
                processed += 1


    if dataset_id is not None and processed == 0:
        raise Exception(f"Unable to find dataset with id {dataset_id}")




if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id",default=None)

    args = parser.parse_args()

    update_end_date(args.dataset_id)