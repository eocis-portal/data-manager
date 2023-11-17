# -*- coding: utf-8 -*-

#    EOCIS data-processor
#    Copyright (C) 2023  National Centre for Earth Observation (NCEO)
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

import copy
import os
import logging
import zipfile

from .store import Store
from .job_operations import JobOperations
from .schema_operations import SchemaOperations
from .task import Task
from .config import Config


class JobManager:
    """Perform various job management tasks"""

    def __init__(self, store:Store):
        """
        Create a job manager
        :param store: the persistent store
        """
        self.store = store
        self.logger = logging.getLogger("JobManager")

    def create_tasks(self, job_id:str):
        """
        Create one or more tasks that need to be executed for a given job

        :param job_id: the id of the job in the persistent store
        """
        self.logger.info(f"Creating tasks for job {job_id}")
        with JobOperations(self.store) as jo:
            job = jo.get_job(job_id)
            job_spec = job.get_spec()
            start_year = int(job_spec["START_YEAR"])
            end_year = int(job_spec["END_YEAR"])
            bundle_id = job_spec["BUNDLE_ID"]
            with SchemaOperations(Store()) as so:
                bundle = so.get_bundle(bundle_id)

            # get a list of (dataset_id, variable_id) tuples
            variables = list(map(lambda v: tuple(v.split(":")), job_spec["VARIABLES"]))
            dataset_ids = set()
            output_path = os.path.join(Config.OUTPUT_PATH,job_id)
            os.makedirs(output_path, exist_ok=True)

            for (dataset_id, variable_id) in variables:
                dataset_ids.add(dataset_id)

            for task_dataset_id in dataset_ids:
                task_variables = []

                for (dataset_id, variable_id) in variables:
                    if dataset_id == task_dataset_id:
                        task_variables.append(variable_id)

                with SchemaOperations(Store()) as so:
                    dataset = so.get_dataset(dataset_id)
                    dataset_inpath = dataset.location

                dataset_metadata = dataset.spec.get("metadata",{})
                level = dataset_metadata.get("level","LEVEL")
                product = dataset_metadata.get("product","PRODUCT")
                version = dataset_metadata.get("version","VERSION")

                # instantiate non-datetime parts of an output filename pattern
                output_name_pattern = Config.OUTPUT_FILENAME_PATTERN \
                    .replace("{LEVEL}",level).replace("{PRODUCT}",product).replace("{VERSION}",version)

                # create a task to compute the results for each year that the job includes
                for year in range(start_year, end_year+1):
                    # build a specification for this task
                    task_spec = copy.deepcopy(job_spec)
                    # if not the first year in the job, include data from January 1st
                    if year > start_year:
                        task_spec["START_MONTH"] = "1"
                        task_spec["START_DAY"] = "1"
                    # if not the last year in the job, include data up to December 31st
                    if year < end_year:
                        task_spec["END_MONTH"] = "12"
                        task_spec["END_DAY"] = "31"
                    task_spec["VARIABLES"] = task_variables
                    task_spec["IN_PATH"] = dataset_inpath.replace("{YEAR}", str(year))
                    task_spec["OUT_PATH"] = os.path.join(output_path, str(year))
                    task_spec["START_YEAR"] = task_spec["END_YEAR"] = str(year)
                    task_spec["OUTPUT_NAME_PATTERN"] = output_name_pattern
                    task_spec["OUTPUT_FORMAT"] = job_spec["OUTPUT_FORMAT"]
                    if "LON_MIN" not in task_spec:
                        task_spec["LON_MIN"] = bundle.spec.get("bounds",{}).get("minx",-180)
                    if "LON_MAX" not in task_spec:
                        task_spec["LON_MAX"] = bundle.spec.get("bounds",{}).get("maxx",180)
                    if "LAT_MIN" not in task_spec:
                        task_spec["LAT_MIN"] = bundle.spec.get("bounds",{}).get("miny",-90)
                    if "LAT_MAX" not in task_spec:
                        task_spec["LAT_MAX"] = bundle.spec.get("bounds",{}).get("maxy",90)

                    # create a new task
                    task = Task.create(task_spec,job_id)
                    # persist it
                    jo.create_task(task)
                    # queue for execution
                    jo.queue_task(job_id, task.get_task_name())
                    self.logger.info(f"Created task {task.get_task_name()} for job {job_id}")

    def zip_results(self, task:Task) -> str:
        """
        When a task is complete, zip up the output files

        :param task: the completed task
        :return: the path of the created zip file
        """
        output_path = os.path.join(Config.OUTPUT_PATH, task.get_job_id())
        year = task.spec["END_YEAR"]
        zip_path = os.path.join(output_path,year+".zip")
        task_out_path = task.spec["OUT_PATH"]
        with zipfile.ZipFile(zip_path, 'w') as outz:
            for file_name in os.listdir(task_out_path):
                file_path = os.path.join(task_out_path,file_name)
                outz.write(file_path, file_name)
                os.remove(file_path)
            os.rmdir(task_out_path)
        return zip_path

    def update_job(self, job_id:str):
        """
        When a task has been completed or failed, update the job's status if there are no remaining tasks
        :param job_id: the ID of the job to update

        TODO need some logic to unqueue all remaining queued tasks if even one task has failed
        """
        with JobOperations(self.store) as jo:
            new_running_count = jo.count_tasks_by_state([Task.STATE_NEW, Task.STATE_RUNNING], job_id=job_id)
            self.logger.info(f"Job {job_id} has {new_running_count} active tasks")
            job = jo.get_job(job_id)
            if new_running_count == 0:
                failed_count = jo.count_tasks_by_state([Task.STATE_FAILED], job_id=job_id)
                if failed_count == 0:
                    job.set_completed()
                    self.logger.info(f"Job {job_id} completed")
                else:
                    job.set_failed(f"{failed_count} tasks failed")
                    self.logger.info(f"Job {job_id} failed with {failed_count} failed tasks")
            else:
                job.set_running()
            jo.update_job(job)

