

from eocis_data_manager.store import Store
from eocis_data_manager.job_operations import JobOperations

if __name__ == '__main__':
    store = Store()
    with JobOperations(store) as jo:
        jo.clear_task_queue()
        jo.removeAllTasks()
        jo.removeAllJobs()
