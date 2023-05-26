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

from eocis_data_manager.store import Store, Transaction

from eocis_data_manager.task import Task
from eocis_data_manager.job import Job

class JobOperations(Transaction):

    def __init__(self, store):
        super().__init__(store)

    def computeSummary(self):

        curs = self.conn.cursor()

        curs.execute("SELECT 'JOB' AS 'TYPE', state AS STATE, COUNT(*) AS COUNT FROM jobs GROUP BY state" +
                     " UNION " +
                     "SELECT 'TASK' AS 'TYPE', state AS STATE, COUNT(*) AS COUNT FROM tasks GROUP BY state")
        return self.collectResults(curs)

    def createJob(self, job):
        """
        creates a job job

        :param job: the job object
        """
        curs = self.conn.cursor()
        curs.execute(
            "INSERT INTO jobs(job_id, submission_date, submitter_id, spec, state, completion_date) values (%s,%s,%s,%s,%s,%s)",
            (
                job.getJobId(),
                Store.encodeDateTime(job.getSubmissionDateTime()),
                job.getSubmitterId(),
                json.dumps(job.getSpec()),
                job.getState(),
                Store.encodeDateTime(job.getCompletionDateTime())
            ))
        return self

    def updateJob(self, job):
        """
        updates an existing job

        :param job: the job object
        """
        curs = self.conn.cursor()
        curs.execute(
            "UPDATE jobs SET submission_date=%s,completion_date=%s,state=%s WHERE job_id=%s",
            (Store.encodeDateTime(job.getSubmissionDateTime()),
             Store.encodeDateTime(job.getCompletionDateTime()),
             job.getState(),
             job.getJobId()))
        return self

    def createTask(self, task):
        """
        stores a new task

        :param task: the task object
        """
        curs = self.conn.cursor()
        curs.execute(
            "INSERT INTO tasks(parent_job_id, task_name, submission_date, remote_task_id, spec, state, completion_date, error, retry_count) values (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
            (
                task.getJobId(),
                task.getTaskName(),
                Store.encodeDateTime(task.getSubmissionDateTime()),
                task.getRemoteId(),
                json.dumps(task.getSpec()),
                task.getState(),
                Store.encodeDateTime(task.getCompletionDateTime()),
                task.getError(),
                task.getRetryCount()))
        return self

    def getTask(self, job_id, task_name):
        """
        retrieve and return a task given its job ID and task name.  Return None if no matching job found
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM tasks WHERE parent_job_id = %s and task_name = %s", (job_id,task_name))

        tasks = self.collectTasks(self.collectResults(curs))
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
        results = self.collectResults(curs)
        if len(results) == 0:
            return None
        else:
            task_name = results[0]["task_name"]
            job_id = results[0]["job_id"]
            return self.getTask(job_id, task_name)


    def updateTask(self, task):
        """
        updates an existing task

        :param task: the task object
        """
        curs = self.conn.cursor()
        curs.execute(
            "UPDATE tasks SET submission_date=%s,completion_date=%s,error=%s,state=%s,remote_task_id=%s,retry_count=%s WHERE parent_job_id=%s AND task_name=%s",
            (Store.encodeDateTime(task.getSubmissionDateTime()),
             Store.encodeDateTime(task.getCompletionDateTime()),
             task.getError(),
             task.getState(),
             task.getRemoteId(),
             task.getRetryCount(),
             task.getJobId(),
             task.getTaskName()))
        return self

    def resetRunningTasks(self):
        """
        mark all running tasks as new.
        """
        curs = self.conn.cursor()
        curs.execute("UPDATE tasks SET state='NEW' WHERE state='RUNNING'")

    def removeJob(self, job_id):
        """
        delete a job and all its tasks

        :param job_id: the id of the job
        """
        curs = self.conn.cursor()
        curs.execute("DELETE FROM jobs WHERE job_id=%s", (job_id,))
        # foreign key from tasks(parent_job_id) => jobs(job_id) should ensure child tasks are deleted

    def removeAllJobs(self):
        curs = self.conn.cursor()
        curs.execute("DELETE FROM jobs;")

    def removeTasksForJob(self, job_id):
        """
        delete all tasks belonging to a job

        :param job_id: the id of the job
        """
        curs = self.conn.cursor()
        curs.execute("DELETE FROM tasks WHERE parent_job_id=%s", (job_id,))

    def removeAllTasks(self):
        curs = self.conn.cursor()
        curs.execute("DELETE FROM tasks;")

    def existsJob(self, job_id):
        """
        check if a job exists

        :param job_id: the id of the job
        """

        curs = self.conn.cursor()
        curs.execute("SELECT job_id FROM jobs WHERE job_id=%s", (job_id,))
        return len(curs.fetchall()) > 0

    def listJobs(self, states=None):
        """
        list all stored jobs
        """

        curs = self.conn.cursor()
        if states:
            curs.execute("SELECT * FROM jobs WHERE state IN (%s)" % (Store.renderValueList(states)))
        else:
            curs.execute("SELECT * FROM jobs")
        return self.collectJobs(self.collectResults(curs))

    def getJob(self, job_id):
        """
        retrieve and return a job given its ID.  Return None if no matching job found
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))

        jobs = self.collectJobs(self.collectResults(curs))
        if len(jobs) == 0:
            return None
        else:
            return jobs[0]

    def listJobsBySubmitterId(self, submitter_id):
        """
        list all stored jobs
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM jobs WHERE submitter_id = %s ORDER BY submission_date", (submitter_id,))
        return self.collectJobs(self.collectResults(curs))

    def listTasks(self, states=None):
        """
        return a list of (task,submitter_id,job_state) tuples, ordered by the submission date of the parent job
        """
        curs = self.conn.cursor()
        if states:
            curs.execute(
                "SELECT T.*, J.submitter_id, J.state FROM tasks T, jobs J WHERE T.state IN (%s) AND T.parent_job_id = J.job_id ORDER BY J.submission_date" % (
                    Store.renderValueList(states)))
        else:
            curs.execute(
                "SELECT T.*, J.submitter_id, J.state FROM tasks T, jobs J WHERE T.parent_job_id = J.job_id ORDER BY J.submission_date")
        results = self.collectResults(curs)
        return list(zip(self.collectTasks(results), map(lambda x: x[Store.JOB_SUBMITTER_ID], results),
                        map(lambda x: x[Store.JOB_STATE], results)))

    def listJobTasks(self, job_id):
        """
        list all tasks associated with a job
        """
        curs = self.conn.cursor()
        curs.execute("SELECT * FROM tasks WHERE parent_job_id = %s", (job_id,))
        return self.collectTasks(self.collectResults(curs))

    def collectTasks(self, results):
        tasks = []
        for row in results:
            task = Task(row[Store.TASK_PARENT_JOB_ID], row[Store.TASK_TASK_NAME], json.loads(row[Store.TASK_SPEC]))
            task \
                .setCompletionDateTime(Store.decodeDate(row[Store.TASK_COMPLETION_DATE])) \
                .setSubmissionDateTime(Store.decodeDate(row[Store.TASK_SUBMISSION_DATE])) \
                .setError(row[Store.TASK_ERROR]) \
                .setRemoteId(row[Store.TASK_REMOTE_TASK_ID]) \
                .setState(row[Store.TASK_STATE]) \
                .setRetryCount(row[Store.TASK_RETRY_COUNT])
            tasks.append(task)
        return tasks

    def collectJobs(self, results):
        jobs = []
        for row in results:
            job = Job(row[Store.JOB_JOB_ID], row[Store.JOB_SUBMITTER_ID], json.loads(row[Store.JOB_SPEC]))
            job \
                .setCompletionDateTime(Store.decodeDateTime(row[Store.JOB_COMPLETION_DATE])) \
                .setSubmissionDateTime(Store.decodeDateTime(row[Store.JOB_SUBMISSION_DATE])) \
                .setState(row[Store.JOB_STATE]) \
                .setError(row[Store.JOB_ERROR])
            jobs.append(job)
        return jobs

    def countJobsByState(self, states):
        """
        Arguments:
        :param states: list of the states of interest from ("NEW","RUNNING","COMPLETED","FAILED")

        :return: the number of jobs with the given state
        """

        curs = self.conn.cursor()
        curs.execute("SELECT COUNT(*) FROM jobs WHERE state IN (%s)" % (Store.renderValueList(states)))
        return curs.fetchone()[0]

    def countTasksByState(self, states, job_id=None):
        """
        Arguments:
        :param states: list of the states of interest from ("NEW","RUNNING","COMPLETED","FAILED")

        Keyword Arguments:
        :param job_id: only count tasks associated with this job id, if provided

        :return: the number of tasks with the given state(s)
        """
        curs = self.conn.cursor()
        states_string = Store.renderValueList(states)
        if job_id:

            curs.execute("select COUNT(*) FROM tasks WHERE state IN (" + states_string + ") AND parent_job_id = %s", (job_id,))
        else:
            curs.execute("select COUNT(*) FROM tasks WHERE state IN (" + states_string + ")")
        return curs.fetchone()[0]

    def countTaskErrors(self, job_id):
        """
        Arguments:
        :param job_id: job id to check tasks

        :return: the number of tasks from the specified job that completed with an error
        """
        curs = self.conn.cursor()
        curs.execute("select COUNT(*) FROM tasks WHERE error <> '' AND parent_job_id = ?", (job_id,))
        return curs.fetchone()[0]