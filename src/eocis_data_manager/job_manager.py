
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

    def __init__(self, store:Store):
        self.store = store
        self.logger = logging.getLogger("JobManager")

    def create_tasks(self, job_id:str):
        self.logger.info(f"Creating tasks for job {job_id}")
        with JobOperations(self.store) as jo:
            job = jo.get_job(job_id)
            job_spec = job.get_spec()
            start_year = int(job_spec["START_YEAR"])
            end_year = int(job_spec["END_YEAR"])
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

                output_name_pattern = "{Y}{m}{d}{H}{M}{S}-EOCIS-{LEVEL}-{PRODUCT}-v{VERSION}-fv01.0" \
                    .replace("{LEVEL}",level).replace("{PRODUCT}",product).replace("{VERSION}",version)

                for year in range(start_year, end_year+1):
                    task_spec = copy.deepcopy(job_spec)
                    if year > start_year:
                        task_spec["START_MONTH"] = "1"
                        task_spec["START_DAY"] = "1"
                    if year < end_year:
                        task_spec["END_MONTH"] = "12"
                        task_spec["END_DAY"] = "31"
                    task_spec["VARIABLES"] = task_variables
                    task_spec["IN_PATH"] = dataset_inpath.replace("{YEAR}", str(year))
                    task_spec["OUT_PATH"] = os.path.join(output_path, str(year))
                    task_spec["START_YEAR"] = task_spec["END_YEAR"] = str(year)
                    task_spec["OUTPUT_NAME_PATTERN"] = output_name_pattern
                    task = Task.create(task_spec,job_id)
                    jo.create_task(task)
                    jo.queue_task(job_id, task.get_task_name())
                    self.logger.info(f"Created task {task.get_task_name()} for job {job_id}")

    def zip_results(self, task:Task):
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

    def update_job(self, job_id:str):

        with JobOperations(self.store) as jo:
            new_running_count = jo.count_tasks_by_state([Task.STATE_NEW, Task.STATE_RUNNING], job_id=job_id)
            self.logger.info(f"Job {job_id} has {new_running_count} active tasks")
            if new_running_count == 0:
                failed_count = jo.count_tasks_by_state([Task.STATE_FAILED], job_id=job_id)
                job = jo.get_job(job_id)
                if failed_count == 0:
                    job.set_completed()
                    self.logger.info(f"Job {job_id} completed")
                else:
                    job.set_failed(f"{failed_count} tasks failed")
                    self.logger.info(f"Job {job_id} failed with {failed_count} failed tasks")

                jo.update_job(job)

    def collect_download_links(self, job_id:str):
        links = []
        output_path = os.path.join(Config.OUTPUT_PATH, job_id)
        for filename in os.listdir(output_path):
            if filename.endswith(".zip"):
                links.append((filename,"/outputs/"+job_id+"/"+filename))
        return links

