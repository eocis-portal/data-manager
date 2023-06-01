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

import json

from eocis_data_manager.store import Store
from eocis_data_manager.transaction import Transaction

from eocis_data_manager.task import Task
from eocis_data_manager.job import Job

class JobOperations(Transaction):

    def __init__(self, store):
        super().__init__(store)

    def compute_summary(self):

        curs = self.conn.cursor()

        curs.execute("SELECT 'JOB' AS 'TYPE', state AS STATE, COUNT(*) AS COUNT FROM jobs GROUP BY state" +
                     " UNION " +
                     "SELECT 'TASK' AS 'TYPE', state AS STATE, COUNT(*) AS COUNT FROM tasks GROUP BY state")
        return self.collect_results(curs)

    def create_job(self, job):
        """
        creates a job job

        :param job: the job object
        """
        curs = self.conn.cursor()
        curs.execute(
            "INSERT INTO jobs(job_id, submission_date, submitter_id, spec, state, completion_date) values (%s,%s,%s,%s,%s,%s)",
            (
                job.get_job_id(),
                Store.encode_datetime(job.get_submission_datetime()),
                job.get_submitter_id(),
                json.dumps(job.get_spec()),
                job.get_state(),
                Store.encode_datetime(job.get_completion_datetime())
            ))
        return self

    def update_job(self, job):
        """
        updates an existing job

        :param job: the job object
        """
        curs = self.conn.cursor()
        curs.execute(
            "UPDATE jobs SET submission_date=%s,completion_date=%s,state=%s WHERE job_id=%s",
            (Store.encode_datetime(job.get_submission_datetime()),
             Store.encode_datetime(job.get_completion_datetime()),
             job.get_state(),
             job.get_job_id()))
        return self

    def create_task(self, task):
        """
        stores a new task

        :param task: the task object
        """
        curs = self.conn.cursor()
        curs.execute(
            "INSERT INTO tasks(parent_job_id, task_name, submission_date, spec, state, completion_date, error, retry_count) values (%s,%s,%s,%s,%s,%s,%s,%s);",
            (
                task.get_job_id(),
                task.get_task_name(),
                Store.encode_datetime(task.get_submission_datetime()),
                json.dumps(task.get_spec()),
                task.get_state(),
                Store.encode_datetime(task.get_completion_datetime()),
                task.get_error(),
                task.get_retry_count()))
        return self

    def get_task(self, job_id, task_name):
        """
        retrieve and return a task given its job ID and task name.  Return None if no matching job found
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM tasks WHERE parent_job_id = %s and task_name = %s", (job_id,task_name))

        tasks = self.collect_tasks(self.collect_results(curs))
        if len(tasks) == 0:
            return None
        else:
            return tasks[0]

    def queue_task(self, job_id, task_name):
        curs = self.conn.cursor()
        curs.execute(
            """INSERT INTO task_queue(job_id, task_name) VALUES (%s, %s);""",
            (job_id, task_name)
        )

    def clear_task_queue(self):
        curs = self.conn.cursor()
        curs.execute(
            """DELETE FROM task_queue;"""
        )

    def get_queued_taskids(self):
        curs = self.conn.cursor()
        curs.execute("SELECT job_id, task_name FROM task_queue;")
        return list(map(lambda row: (row["job_id"],row["task_name"]), self.collect_results(curs)))

    def get_next_task(self):
        curs = self.conn.cursor()
        curs.execute(
            """DELETE FROM task_queue 
                WHERE id = (
                  SELECT id
                  FROM task_queue
                  ORDER BY queue_time ASC 
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                RETURNING *;"""
        )
        results = self.collect_results(curs)
        if len(results) == 0:
            return None
        else:
            task_name = results[0]["task_name"]
            job_id = results[0]["job_id"]
            return self.get_task(job_id, task_name)


    def update_task(self, task):
        """
        updates an existing task

        :param task: the task object
        """
        curs = self.conn.cursor()
        curs.execute(
            "UPDATE tasks SET submission_date=%s,completion_date=%s,error=%s,state=%s,retry_count=%s WHERE parent_job_id=%s AND task_name=%s",
            (Store.encode_datetime(task.get_submission_datetime()),
             Store.encode_datetime(task.get_completion_datetime()),
             task.get_error(),
             task.get_state(),
             task.get_retry_count(),
             task.get_job_id(),
             task.get_task_name()))
        return self

    def reset_running_tasks(self):
        """
        mark all running tasks as new.
        """
        curs = self.conn.cursor()
        curs.execute("UPDATE tasks SET state='NEW' WHERE state='RUNNING'")

    def remove_job(self, job_id):
        """
        delete a job and all its tasks

        :param job_id: the id of the job
        """
        curs = self.conn.cursor()
        curs.execute("DELETE FROM jobs WHERE job_id=%s", (job_id,))
        # foreign key from tasks(parent_job_id) => jobs(job_id) should ensure child tasks are deleted

    def remove_all_jobs(self):
        curs = self.conn.cursor()
        curs.execute("DELETE FROM jobs;")

    def remove_tasks_for_job(self, job_id):
        """
        delete all tasks belonging to a job

        :param job_id: the id of the job
        """
        curs = self.conn.cursor()
        curs.execute("DELETE FROM tasks WHERE parent_job_id=%s", (job_id,))

    def remove_all_tasks(self):
        curs = self.conn.cursor()
        curs.execute("DELETE FROM tasks;")

    def job_exists(self, job_id):
        """
        check if a job exists

        :param job_id: the id of the job
        """

        curs = self.conn.cursor()
        curs.execute("SELECT job_id FROM jobs WHERE job_id=%s", (job_id,))
        return len(curs.fetchall()) > 0

    def list_jobs(self, states=None):
        """
        list all stored jobs
        """

        curs = self.conn.cursor()
        if states:
            curs.execute("SELECT * FROM jobs WHERE state IN (%s)" % (Store.render_value_list(states)))
        else:
            curs.execute("SELECT * FROM jobs")
        return self.collect_jobs(self.collect_results(curs))

    def get_job(self, job_id):
        """
        retrieve and return a job given its ID.  Return None if no matching job found
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))

        jobs = self.collect_jobs(self.collect_results(curs))
        if len(jobs) == 0:
            return None
        else:
            return jobs[0]

    def list_jobs_by_submitter_id(self, submitter_id):
        """
        list all stored jobs
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM jobs WHERE submitter_id = %s ORDER BY submission_date", (submitter_id,))
        return self.collect_jobs(self.collect_results(curs))

    def list_tasks(self, states=None):
        """
        return a list of (task,submitter_id,job_state) tuples, ordered by the submission date of the parent job
        """
        curs = self.conn.cursor()
        if states:
            curs.execute(
                "SELECT T.*, J.submitter_id, J.state FROM tasks T, jobs J WHERE T.state IN (%s) AND T.parent_job_id = J.job_id ORDER BY J.submission_date" % (
                    Store.render_value_list(states)))
        else:
            curs.execute(
                "SELECT T.*, J.submitter_id, J.state FROM tasks T, jobs J WHERE T.parent_job_id = J.job_id ORDER BY J.submission_date")
        results = self.collect_results(curs)
        return list(zip(self.collect_tasks(results), map(lambda x: x[Store.JOB_SUBMITTER_ID], results),
                        map(lambda x: x[Store.JOB_STATE], results)))

    def list_job_tasks(self, job_id):
        """
        list all tasks associated with a job
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM tasks WHERE parent_job_id = %s", (job_id,))
        return self.collect_tasks(self.collect_results(curs))

    def collect_tasks(self, results):
        tasks = []
        for row in results:
            task = Task(row[Store.TASK_PARENT_JOB_ID], row[Store.TASK_TASK_NAME], json.loads(row[Store.TASK_SPEC]))
            task \
                .set_completion_datetime(Store.decode_datetime(row[Store.TASK_COMPLETION_DATE])) \
                .set_submission_datetime(Store.decode_datetime(row[Store.TASK_SUBMISSION_DATE])) \
                .set_error(row[Store.TASK_ERROR]) \
                .set_state(row[Store.TASK_STATE]) \
                .set_retry_count(row[Store.TASK_RETRY_COUNT])
            tasks.append(task)
        return tasks

    def collect_jobs(self, results):
        jobs = []
        for row in results:
            job = Job(row[Store.JOB_JOB_ID], row[Store.JOB_SUBMITTER_ID], json.loads(row[Store.JOB_SPEC]))
            job \
                .set_completion_datetime(Store.decode_datetime(row[Store.JOB_COMPLETION_DATE])) \
                .set_submission_datetime(Store.decode_datetime(row[Store.JOB_SUBMISSION_DATE])) \
                .set_state(row[Store.JOB_STATE]) \
                .set_error(row[Store.JOB_ERROR])
            jobs.append(job)
        return jobs

    def count_jobs_by_state(self, states):
        """
        Arguments:
        :param states: list of the states of interest from ("NEW","RUNNING","COMPLETED","FAILED")

        :return: the number of jobs with the given state
        """

        curs = self.conn.cursor()
        curs.execute("SELECT COUNT(*) FROM jobs WHERE state IN (%s)" % (Store.render_value_list(states)))
        return curs.fetchone()[0]

    def count_tasks_by_state(self, states, job_id=None):
        """
        Arguments:
        :param states: list of the states of interest from ("NEW","RUNNING","COMPLETED","FAILED")

        Keyword Arguments:
        :param job_id: only count tasks associated with this job id, if provided

        :return: the number of tasks with the given state(s)
        """
        curs = self.conn.cursor()
        states_string = Store.render_value_list(states)
        if job_id:

            curs.execute("select COUNT(*) FROM tasks WHERE state IN (" + states_string + ") AND parent_job_id = %s", (job_id,))
        else:
            curs.execute("select COUNT(*) FROM tasks WHERE state IN (" + states_string + ")")
        return curs.fetchone()[0]

    def count_task_errors(self, job_id):
        """
        Arguments:
        :param job_id: job id to check tasks

        :return: the number of tasks from the specified job that completed with an error
        """
        curs = self.conn.cursor()
        curs.execute("select COUNT(*) FROM tasks WHERE error <> '' AND parent_job_id = ?", (job_id,))
        return curs.fetchone()[0]