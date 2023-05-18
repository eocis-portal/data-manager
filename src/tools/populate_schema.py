
import xarray as xr
import datetime

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations

def populate_schema(from_path):
    store = Store()
    with SchemaOperations(store) as ops:
        ops.populate_schema(from_path)

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("schema_folder")

    args = parser.parse_args()

    populate_schema(args.schema_folder)