
import xarray as xr
import datetime

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations

def update_end_date(dataset_id):
    store = Store()
    location = ""
    with SchemaOperations(store) as ops:
        datasets = ops.listDataSets()
        for dataset in datasets:
            if dataset.dataset_id == dataset_id:
                location = dataset.location

    if not location:
        raise Exception(f"Unable to find dataset with id {dataset_id}")

    # this can take some time
    ds = xr.open_mfdataset(location)

    last_ts = ds["time"].values.tolist()[-1]

    end_dt = datetime.datetime.fromtimestamp(last_ts/1e9).date()
    print("End Date: "+Store.encodeDate(end_dt))
    with SchemaOperations(store) as ops:
        ops.updateEndDate(dataset_id, end_dt)


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_name")

    args = parser.parse_args()

    update_end_date(args.dataset_name)