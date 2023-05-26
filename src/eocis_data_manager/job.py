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
from .task import Task

import os

from .config import Config
import json

class Job:
    """
    Represents a job - a user actioned piece of work that is accomplished by executing zero or more tasks

    Jobs have a state, submission and completion times.
    """

    @staticmethod
    def create(spec, job_id=""):
        """factory method to create and return a freshly submitted job based on an email address and specification"""
        job_id = job_id or str(uuid.uuid4())
        job = Job(job_id,spec["SUBMITTER_ID"],spec)
        job.setSubmissionDateTime(datetime.datetime.now(datetime.timezone.utc))
        return job

    def __init__(self, job_id, submitter_id, spec):
        self.job_id = job_id
        self.submitter_id = submitter_id
        self.spec = spec
        self.state = Job.STATE_NEW
        self.submission_date_time = None
        self.completion_date_time = None
        self.error = ""

    def getJobId(self):
        return self.job_id

    def setRunning(self):
        """Move this job into the RUNNING state. This transition is usually triggered when the job's tasks have been created."""
        self.setState(Job.STATE_RUNNING)
        return self

    def setCompleted(self):
        """Move this job into the COMPLETED state, noting the current UTC date/time as its completed date"""
        self.setState(Job.STATE_COMPLETED).setCompletionDateTime(datetime.datetime.now(datetime.timezone.utc))
        return self

    def setFailed(self,error=""):
        """Move this job into the FAILED state, noting the error and the current UTC date/time as its completed date"""
        self.setState(Job.STATE_FAILED).setCompletionDateTime(datetime.datetime.now(datetime.timezone.utc)).setError(error)
        return self

    def getSubmitterId(self):
        return self.submitter_id

    def getSpec(self):
        return self.spec

    def getSubmissionDateTime(self):
        return self.submission_date_time

    def setSubmissionDateTime(self, submission_date_time):
        self.submission_date_time = submission_date_time
        return self

    def getCompletionDateTime(self):
        return self.completion_date_time

    def setCompletionDateTime(self, completion_date_time):
        self.completion_date_time = completion_date_time
        return self

    def getState(self):
        return self.state

    def setState(self, state):
        self.state = state
        return self

    def getError(self):
        return self.error

    def setError(self,error):
        self.error = error
        return self

    def getDurationHours(self):
        if self.state == Job.STATE_NEW or self.state == Job.STATE_RUNNING:
            return (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - self.getSubmissionDateTime()).total_seconds()/3600
        else:
            return (self.getCompletionDateTime() - self.getSubmissionDateTime()).total_seconds()/3600

    def getExpiryDate(self):
        if self.state not in [Job.STATE_COMPLETED,Job.STATE_FAILED]:
            return None
        return self.getCompletionDateTime()+datetime.timedelta(seconds=Config.CLEANUP_AFTER_SECS)

    STATE_NEW = "NEW"
    STATE_RUNNING = "RUNNING"
    STATE_COMPLETED = "COMPLETED"
    STATE_FAILED = "FAILED"

    def __repr__(self):
        status = self.getState()
        if status == Job.STATE_FAILED:
            status += "(%s)" % (self.getError())
        return "%s %s %s %0.2f hours"%(self.getJobId(),self.getSubmitterId(),status,self.getDurationHours())

    def dump(self):
        attrs = {
            "id":           self.getJobId(),
            "submitter":    self.submitter_id,
            "spec":         json.dumps(self.getSpec()),
            "state":        self.getState(),
            "submitted":    str(self.getSubmissionDateTime()),
            "completed":    str(self.getCompletionDateTime()),
            "expiry":       str(self.getExpiryDate()),
            "duration":     self.getDurationHours(),
            "error":        self.getError()
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
    def getAllStates():
        return [Job.STATE_NEW,Job.STATE_RUNNING,Job.STATE_COMPLETED,Job.STATE_FAILED]

    def serialise(self,transaction):
        """Serialise and return detailed information about the job and its tasks, using the given database transaction parameter to retrieve associated task information from the database"""
        data = {}
        data["id"] = self.job_id
        data["state"] = self.state
        data["error"] = self.getError()
        data["duration"]  = self.getDurationHours()
        data["submission_date"] = str(self.getSubmissionDateTime())
        data["completion_date"] = str(self.getCompletionDateTime()) if self.state == Job.STATE_COMPLETED else ""
        data["duration"] = self.getDurationHours()
        data["new_tasks"] = transaction.countTasksByState([Task.STATE_NEW],self.getJobId())
        data["running_tasks"] = transaction.countTasksByState([Task.STATE_RUNNING],self.getJobId())
        data["completed_tasks"] = transaction.countTasksByState([Task.STATE_COMPLETED],self.getJobId())
        data["failed_tasks"] = transaction.countTasksByState([Task.STATE_FAILED],self.getJobId())
        data["expiry_date"] = str(self.getExpiryDate()) if self.state in [Job.STATE_COMPLETED,Job.STATE_FAILED] else ""
        return data
