
import os
import tempfile
import shutil

from eocis_data_manager.store import Store

class StoreTest:

    def __init__(self):
        self.path = tempfile.NamedTemporaryFile(suffix=".db").name
        print(self.path)

    def get_store(self):
        return Store("dbname=eocistest user=eocistest")

    def __enter__(self):
        os.system(f'initdb -D "{self.path}"')
        os.system(f'pg_ctl -D "{self.path}" -l "{self.path}/log.log" start')
        os.system(f'createuser --encrypted eocistest')
        os.system(f'createdb --owner=eocistest eocistest')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.system(f'pg_ctl -D "{self.path}" -l "{self.path}/log.log" stop')
        shutil.rmtree(self.path)

if __name__ == '__main__':
    with StoreTest() as s:
        store = s.get_store()
