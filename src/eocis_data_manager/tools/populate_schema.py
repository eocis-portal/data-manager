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

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations

folder = os.path.split(__file__)[0]

def populate_schema(from_path:str):
    """
    Reload the schema tables (bundles, datasets, variables) from YAML files

    :param from_path: folder containing bundles and datasets sub-directories containing YAML files
    """
    store = Store()
    with SchemaOperations(store) as ops:
        # save the old end dates for each dataset
        end_dates = ops.get_dataset_end_dates()
        # rebuild the schema (with empty end dates)
        ops.populate_schema(from_path)
        # restore the original end dates for each dataset (if they exist in the new schema)
        for (dataset_id, end_date) in end_dates.items():
            ops.update_dataset_end_date(dataset_id, end_date)

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-folder", default=os.path.join(folder,"..","..","..","schema"))

    args = parser.parse_args()

    populate_schema(args.schema_folder)