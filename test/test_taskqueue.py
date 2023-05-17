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


import unittest

from eocis_data_manager.job_operations import JobOperations
from store_test import StoreTest


class TestTaskQueue(unittest.TestCase):

    def test_taskqueue(self):
        """Check that the task queue system works, at least from a single execution thread"""

        with StoreTest() as st:
            s = st.get_store()
            # add three jobs to the queue
            with JobOperations(s) as t:
                # load the schema into the database
                t.queue_task("job0","task0")
                t.queue_task("job1","task1")
                t.queue_task("job2","task2")

            # dequeue the first
            with JobOperations(s) as t:
                next_task = t.get_next_task()
                self.assertEqual(next_task["job_id"],"job0")
                self.assertEqual(next_task["task_name"], "task0")

            # dequeue the second
            with JobOperations(s) as t:
                next_task = t.get_next_task()
                self.assertEqual(next_task["job_id"],"job1")
                self.assertEqual(next_task["task_name"], "task1")

            # dequeue the third but rollback
            t = JobOperations(s)
            next_task = t.get_next_task()
            self.assertEqual(next_task["job_id"],"job2")
            self.assertEqual(next_task["task_name"], "task2")
            t.rollback()

            # check we can dequeue the third again
            with JobOperations(s) as t:
                next_task = t.get_next_task()
                self.assertEqual(next_task["job_id"],"job2")
                self.assertEqual(next_task["task_name"], "task2")

            # no more tasks
            with JobOperations(s) as t:
                next_task = t.get_next_task()
                self.assertEqual(next_task,None)

if __name__ == '__main__':
    unittest.main()