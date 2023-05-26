
import copy
import os

from .store import Store
from .job_operations import JobOperations
from .schema_operations import SchemaOperations

from .task import Task

from .config import Config

class JobManager:

    def __init__(self, store):
        self.store = store

    def create_tasks(self, job_id):
        with JobOperations(self.store) as jo:
            job = jo.getJob(job_id)
            job_spec = job.getSpec()
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
                    dataset = so.getDataset(dataset_id)
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
                    jo.createTask(task)
                    jo.queue_task(job_id,task.getTaskName())