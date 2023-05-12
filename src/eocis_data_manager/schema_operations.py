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

import os.path
import json

from eocis_data_manager.store import Store, Transaction

from eocis_data_manager.bundle import Bundle
from eocis_data_manager.dataset import DataSet, Variable

class SchemaOperations(Transaction):

    def __init__(self, store):
        super().__init__(store)

    def clear_schema(self):
        curs = self.conn.cursor()
        curs.execute("DELETE FROM datasets;")
        curs.execute("DELETE FROM bundles;")
        curs.execute("DELETE FROM variables;")
        curs.execute("DELETE FROM dataset_bundle;")

    def populate_schema(self, path):
        self.clear_schema()

        datasets = DataSet.load_datasets(os.path.join(path, "datasets"))
        bundles = Bundle.load_bundles(os.path.join(path, "bundles"))
        for dataset in datasets:
            self.createDataSet(dataset)
        for bundle in bundles:
            self.createBundle(bundle)

        return self

    def createBundle(self, bundle):
        curs = self.conn.cursor()
        curs.execute(
            "INSERT INTO bundles(bundle_id,bundle_name,spec,minx,miny,maxx,maxy) values (?,?,?,?,?,?,?)",
            (
                bundle.bundle_id,
                bundle.bundle_name,
                json.dumps(bundle.spec),
                bundle.minx,
                bundle.miny,
                bundle.maxx,
                bundle.maxy
            ))

        for dataset_id in bundle.dataset_ids:
            curs.execute(
                "INSERT INTO dataset_bundle(bundle_id,dataset_id) values (?,?)",
                (
                    bundle.bundle_id,
                    dataset_id
                ))

    def createDataSet(self, dataset):
        curs = self.conn.cursor()
        curs.execute(
            "INSERT INTO datasets(dataset_id, dataset_name, temporal_resolution, spatial_resolution,start_date,end_date,location,spec) values (?,?,?,?,?,?,?,?)",
            (
                dataset.dataset_id,
                dataset.dataset_name,
                dataset.temporal_resolution,
                dataset.spatial_resolution,
                Store.encodeDate(dataset.start_date),
                Store.encodeDate(dataset.end_date),
                dataset.location,
                json.dumps(dataset.spec),
            ))

        for variable in dataset.variables:
            curs.execute("INSERT INTO variables(variable_id, dataset_id, variable_name,  spec) values (?,?,?,?)",
                         (
                             variable.variable_id,
                             dataset.dataset_id,
                             variable.variable_name,
                             json.dumps(variable.spec)
                         ))

    def listBundles(self):
        """
        list all stored bundles
        """

        curs = self.conn.cursor()
        curs.execute("SELECT * FROM bundles")
        return self.collectBundles(self.collectResults(curs))

    def collectBundles(self, results):
        bundles = []
        for row in results:
            bundle_id = row["bundle_id"]
            curs = self.conn.cursor()
            curs.execute("SELECT * FROM dataset_bundle WHERE bundle_id=?", (bundle_id,))
            dataset_ids = []
            for r in self.collectResults(curs):
                dataset_ids.append(r["dataset_id"])
            b = Bundle(bundle_id, bundle_name=row["bundle_name"], spec=json.loads(row["spec"]), dataset_ids=dataset_ids,
                       minx=row["minx"],miny=row["miny"],maxx=row["maxx"],maxy=row["maxy"])
            bundles.append(b)
        return bundles

    def listDataSets(self):
        """
        list all stored datasets/variables
        """

        curs = self.conn.cursor()
        curs.execute("SELECT * FROM datasets")
        return self.collectDataSets(self.collectResults(curs))

    def collectDataSets(self, results):
        datasets = []
        for row in results:
            dataset_id = row["dataset_id"]
            curs = self.conn.cursor()
            curs.execute("SELECT * FROM variables WHERE dataset_id=?", (dataset_id,))
            variables = []
            for r in self.collectResults(curs):
                v = Variable(r["variable_id"], r["variable_name"], json.loads(r["spec"]))
                variables.append(v)
            d = DataSet(dataset_id, dataset_name=row["dataset_name"],
                        temporal_resolution=row["temporal_resolution"],
                        spatial_resolution=row["spatial_resolution"],
                        start_date=Store.decodeDate(row["start_date"]),
                        end_date=Store.decodeDate(row["end_date"]),
                        location=row["location"],
                        spec=json.loads(row["spec"]), variables=variables)
            datasets.append(d)
        return datasets
