from eocis_data_manager.store import Store
from eocis_data_manager.job_operations import JobOperations
from eocis_data_manager.schema_operations import SchemaOperations

if __name__ == '__main__':
    store = Store()
    with JobOperations(store) as jo:
        jo.wipe()
    with SchemaOperations(store) as so:
        so.wipe()