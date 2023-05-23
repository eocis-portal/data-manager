
import copy

from .job_operations import JobOperations
from .task import Task

class JobManager:

    def __init__(self, store):
        self.store = store


    def create_tasks(self, job_id):
        with JobOperations(self.store) as jo:
            job = jo.getJob(job_id)
            job_spec = job.getSpec()
            start_year = int(job_spec["START_YEAR"])
            end_year = int(job_spec["END_YEAR"])
            for year in range(start_year, end_year+1):
                task_spec = copy.deepcopy(job_spec)
                if year > start_year:
                    task_spec["START_MONTH"] = "1"
                    task_spec["START_DAY"] = "1"
                if year < end_year:
                    task_spec["END_MONTH"] = "12"
                    task_spec["END_DAY"] = "31"
                task_spec["START_YEAR"] = task_spec["END_YEAR"] = str(year)
                task = Task.create(task_spec,job_id)
                jo.createTask(task)
                jo.queue_task(job_id,task.getTaskName())