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

from eocis_data_manager.store import Store
from eocis_data_manager.job_operations import JobOperations
from eocis_data_manager.schema_operations import SchemaOperations

if __name__ == '__main__':
    store = Store()
    print("Schema:")
    with SchemaOperations(store) as so:
        print("\tBundles:")
        for bundle in so.list_bundles():
            print(f"\t\t{bundle}")
        print("\tDatasets:")
        for dataset in so.list_datasets():
            print(f"\t\t{dataset}")

    print("Jobs/Tasks:")
    with JobOperations(store) as jo:
        print("\tJobs:")
        for job in jo.list_jobs():
            print(f"\t\t{job}")
        print("\tTasks:")
        for task in jo.list_tasks():
            print(f"\t\t{task}")
        print("\tTask Queue:")
        for task_id in jo.get_queued_taskids():
            print(f"\t\t{task_id}")