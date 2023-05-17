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
import unittest
import datetime

from eocis_data_manager.store import Store
from eocis_data_manager.schema_operations import SchemaOperations
from eocis_data_manager.dataset import DataSet, Variable
from eocis_data_manager.bundle import Bundle

from store_test import StoreTest

schema_folder = os.path.join(os.path.split(__file__)[0],"schema")

class TestSchema(unittest.TestCase):

    def test_store(self):
        """Check that schema information is faithfully stored and retrieved from the database"""
        # create a new empty database

        with StoreTest() as st:
            s = st.get_store()
            t = SchemaOperations(s)
            # load the schema into the database
            t.populate_schema(schema_folder)

            # retrieve bundle and dataset information
            bundles = t.listBundles()
            self.assertEqual(1,len(bundles))
            datasets = t.listDataSets()
            self.assertEqual(2,len(datasets))
            t.commit()

            # compare bundle and datasets with those loaded directly from file
            datasets_from_file = DataSet.load_datasets(os.path.join(schema_folder, "datasets"))
            bundles_from_file = Bundle.load_bundles(os.path.join(schema_folder, "bundles"))

            self.assert_equal_with_sort(bundles,bundles_from_file,lambda b: b.bundle_id)
            self.assert_equal_with_sort(datasets, datasets_from_file, lambda d: d.dataset_id)

    def assert_equal_with_sort(self,list1,list2,key_fn):
        """Check that two lists contain the identical elements, when sorted according to the specified key"""
        self.assertEqual(len(list1), len(list2))
        sorted_list1 = sorted(list1, key=key_fn)
        sorted_list2 = sorted(list2, key=key_fn)
        for idx in range(len(sorted_list1)):
            self.assertEqual(sorted_list1[idx],sorted_list2[idx])

    def test_load_dataset(self):
        """Check that information is correctly loaded from a dataset YAML specification"""
        dataset_path = os.path.join(schema_folder,"datasets","sst.yaml")
        dataset = DataSet.load_dataset_from_file(dataset_path)
        self.assertEqual(dataset.dataset_id,"sst")
        self.assertEqual(dataset.dataset_name,"Sea Surface Temperatures")
        self.assertEqual(dataset.temporal_resolution, "daily")
        self.assertEqual(dataset.spatial_resolution, "0.05")
        self.assertEqual(dataset.start_date, datetime.date(1981,9,1))
        self.assertEqual(dataset.end_date, datetime.date(2022,12,31))
        self.assertEqual(dataset.location, "/path/to/data")
        self.assertEqual(dataset.variables[0],Variable("sst", "Sea Surface Temperature",{}))
        self.assertEqual(dataset.variables[1],Variable("sst_uncertainty", "Sea Surface Temperature Uncertainty",{}))
        self.assertEqual(dataset.variables[2],Variable("sea_ice_fraction", "Sea Ice Fraction",{}))
        self.assertEqual(dataset.variables[3],Variable("sea_fraction", "Sea Fraction",{}))

    def test_load_bundle(self):
        """Check that information is correctly loaded from a bundle YAML specification"""
        bundle_path = os.path.join(schema_folder,"bundles","ocean.yaml")
        bundle = Bundle.load_bundle_from_file(bundle_path)
        self.assertEqual(bundle.bundle_id,"ocean")
        self.assertEqual(bundle.bundle_name,"Ocean Data Bundle")
        self.assertEqual(bundle.spec, {"key1":"value","key2":"value"})
        self.assertEqual(bundle.dataset_ids,["sst","oc"])


if __name__ == '__main__':
    unittest.main()