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

"""
The job module deals with the representation of a generic job, accomplished by zero or more tasks in order to satisfy a single user request.

A job is uniquely identified by a unique job identifier and is described by a JSON serialisable specification object, usually a dictionary.

The job object records the task state (NEW, RUNNING, COMPLETED, FAILED), an error message (relevant to FAILED tasks), and its submission and completion times.

A job object also stores the email-address of the user requesting the job
"""

import uuid
import datetime
import math
import json
from typing import Any

from .config import Config
from .task import Task

class Job:
    """
    Represents a job - a user actioned piece of work that is accomplished by executing zero or more tasks

    Jobs have a state, submission and completion times.
    """

    @staticmethod
    def create(spec:dict[str,Any], job_id:str="") -> "Job":
        """factory method to create and return a new job

        :param spec: a dictionary providing the job's specification
        :param job_id: the job ID to create or None to create a new job-id
        :return:
        """
        job_id = job_id or str(uuid.uuid4())
        job = Job(job_id,spec["SUBMITTER_ID"],spec)
        job.set_submission_datetime(datetime.datetime.now(datetime.timezone.utc))
        return job

    def __init__(self, job_id:str, submitter_id:str, spec:dict[str,Any]):
        """
        Construct a job

        :param job_id: the job id (should be unique)
        :param submitter_id: the id of the client submitting the job
        :param spec: a dictionary providing the job's specification
        """
        self.job_id = job_id
        self.submitter_id = submitter_id
        self.spec = spec
        self.state = Job.STATE_NEW
        self.submission_date_time = None
        self.completion_date_time = None
        self.error = ""

    def get_job_id(self) -> str:
        return self.job_id

    def set_running(self) -> "Job":
        """Move this job into the RUNNING state. This transition is usually triggered when the job's tasks have been created."""
        self.set_state(Job.STATE_RUNNING)
        return self

    def set_completed(self) -> "Job":
        """Move this job into the COMPLETED state, noting the current UTC date/time as its completed date"""
        self.set_state(Job.STATE_COMPLETED).set_completion_datetime(datetime.datetime.now(datetime.timezone.utc))
        return self

    def set_failed(self, error="") -> "Job":
        """Move this job into the FAILED state, noting the error and the current UTC date/time as its completed date"""
        self.set_state(Job.STATE_FAILED).set_completion_datetime(datetime.datetime.now(datetime.timezone.utc)).set_error(error)
        return self

    def get_submitter_id(self) -> str:
        return self.submitter_id

    def get_spec(self) -> dict[str,Any]:
        return self.spec

    def get_submission_datetime(self) -> datetime.datetime:
        return self.submission_date_time

    def set_submission_datetime(self, submission_date_time:datetime.datetime) -> "Job":
        self.submission_date_time = submission_date_time
        return self

    def get_completion_datetime(self) -> datetime.datetime:
        return self.completion_date_time

    def set_completion_datetime(self, completion_date_time:datetime.datetime) -> "Job":
        self.completion_date_time = completion_date_time
        return self

    def get_state(self) -> str:
        return self.state

    def set_state(self, state:str):
        self.state = state
        return self

    def get_error(self) -> str:
        return self.error

    def set_error(self, error:str) -> "Job":
        self.error = error
        return self

    def get_duration_hours(self) -> float:
        if self.state == Job.STATE_NEW or self.state == Job.STATE_RUNNING:
            return (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - self.get_submission_datetime()).total_seconds() / 3600
        else:
            completed_dt = self.get_completion_datetime()
            if completed_dt is not None:
                return (completed_dt - self.get_submission_datetime()).total_seconds() / 3600
            else:
                return math.nan # should not really be reachable

    def get_expiry_date(self) -> datetime.datetime:
        if self.state not in [Job.STATE_COMPLETED,Job.STATE_FAILED]:
            return None
        return self.get_completion_datetime() + datetime.timedelta(seconds=Config.CLEANUP_AFTER_SECS)

    STATE_NEW = "NEW"
    STATE_RUNNING = "RUNNING"
    STATE_COMPLETED = "COMPLETED"
    STATE_FAILED = "FAILED"

    def __repr__(self):
        status = self.get_state()
        if status == Job.STATE_FAILED:
            status += "(%s)" % (self.get_error())
        return "%s %s %s %0.2f hours"%(self.get_job_id(), self.get_submitter_id(), status, self.get_duration_hours())

    def dump(self):
        attrs = {
            "id":           self.get_job_id(),
            "submitter":    self.submitter_id,
            "spec":         json.dumps(self.get_spec()),
            "state":        self.get_state(),
            "submitted":    str(self.get_submission_datetime()),
            "completed":    str(self.get_completion_datetime()),
            "expiry":       str(self.get_expiry_date()),
            "duration":     self.get_duration_hours(),
            "error":        self.get_error()
        }
        return """Job %(id)s
            \temail:        %(email)s
            \tspec:         %(spec)s
            \tstate:        %(state)s
            \tsubmitted:    %(submitted)s
            \tcompleted:    %(completed)s
            \texpiry:       %(expiry)s
            \tduration hrs: %(duration)0.2f
            \terror:        %(error)s\n\n"""%attrs

    @staticmethod
    def get_all_states():
        return [Job.STATE_NEW,Job.STATE_RUNNING,Job.STATE_COMPLETED,Job.STATE_FAILED]

    def serialise(self,transaction):
        """Serialise and return detailed information about the job and its tasks, using the given database transaction parameter to retrieve associated task information from the database"""
        data = {}
        data["id"] = self.job_id
        data["state"] = self.state
        data["error"] = self.get_error()
        data["duration"]  = self.get_duration_hours()
        data["submission_date"] = str(self.get_submission_datetime())
        data["completion_date"] = str(self.get_completion_datetime()) if self.state == Job.STATE_COMPLETED else ""
        data["duration"] = self.get_duration_hours()
        data["new_tasks"] = transaction.count_tasks_by_state([Task.STATE_NEW], self.get_job_id())
        data["running_tasks"] = transaction.count_tasks_by_state([Task.STATE_RUNNING], self.get_job_id())
        data["completed_tasks"] = transaction.count_tasks_by_state([Task.STATE_COMPLETED], self.get_job_id())
        data["failed_tasks"] = transaction.count_tasks_by_state([Task.STATE_FAILED], self.get_job_id())
        data["expiry_date"] = str(self.get_expiry_date()) if self.state in [Job.STATE_COMPLETED, Job.STATE_FAILED] else ""
        return data
