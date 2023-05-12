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
The task module deals with the representation of a generic task, a smaller unit of work which forms part of a larger logical job.

A task is uniquely identified by a parent job and a unique task name within the job.  It is described by a JSON serialisable specification object, usually a dictionary.

The task object records the task state (NEW, RUNNING, COMPLETED, FAILED), a remote task ID (relevant to RUNNING tasks), an error message (relevant to FAILED tasks), and its submission and completion times.

Methods setRunning, setCompleted, setFailed and retry are used to move a task between states.

Tasks can be retried if they fail, and so also have an associated retry count.
"""
import datetime

class Task:
    """
    Represent a task - a discrete executable piece of work that contributes towards the completion of a job
    """

    def __init__(self,job_id=None,task_name=None,spec=None):
        self.job_id = job_id
        self.task_name = task_name
        self.spec = spec
        self.state = Task.STATE_NEW
        self.error = ""
        self.submission_date_time = None
        self.completion_date_time = None
        self.remote_id = ""
        self.retrycount = 0

    def setRunning(self):
        """Move this task into the RUNNING state, noting the current UTC date/time as its submission date"""
        self.setState(Task.STATE_RUNNING).setSubmissionDateTime(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None))
        return self

    def setCompleted(self):
        """Move this task into the COMPLETED state, noting the current UTC date/time as its completed date"""
        self.setState(Task.STATE_COMPLETED).setCompletionDateTime(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None))
        return self

    def setFailed(self,error=""):
        """Move this task into the FAILED state, noting the error and the current UTC date/time as its completed date"""
        self.setState(Task.STATE_FAILED).setCompletionDateTime(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)).setError(error)
        return self

    def retry(self):
        """Move this task into the NEW state, incrementing the retry count and removing any associated submission/completion dates and error message"""
        self.setState(Task.STATE_NEW).setCompletionDateTime(None).setSubmissionDateTime(None).setRetryCount(self.getRetryCount()+1).setError("")

    def getJobId(self):
        return self.job_id

    def getTaskName(self):
        return self.task_name

    def setTaskName(self,task_name):
        self.task_name = task_name
        return self

    def getError(self):
        return self.error

    def setError(self,error):
        self.error = error
        return self

    def getRetryCount(self):
        return self.retrycount

    def setRetryCount(self,retrycount):
        self.retrycount = retrycount
        return self

    def getSpec(self):
        return self.spec

    def getSubmissionDateTime(self):
        return self.submission_date_time

    def setSubmissionDateTime(self,submission_date_time):
        self.submission_date_time = submission_date_time
        return self

    def getCompletionDateTime(self):
        return self.completion_date_time

    def setCompletionDateTime(self,completion_date_time=None):
        self.completion_date_time = completion_date_time
        return self

    def getState(self):
        return self.state

    def setState(self,state):
        self.state = state
        return self

    def getRemoteId(self):
        return self.remote_id

    def setRemoteId(self,remote_id):
        self.remote_id = remote_id
        return self

    def getDurationHours(self):
        if self.state == Task.STATE_NEW:
            return 0
        if self.state == Task.STATE_RUNNING:
            if self.getSubmissionDateTime() is not None:
                return (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - self.getSubmissionDateTime()).total_seconds()/3600
            else:
                return 0
        return (self.getCompletionDateTime() - self.getSubmissionDateTime()).total_seconds()/3600


    def __repr__(self):
        status = self.getState()
        status_msg = status
        if status == Task.STATE_RUNNING:
            rcount = self.getRetryCount()
            pid = self.getRemoteId()
            status_msg += "(try=%d, pid=%s)"%(rcount,pid)
        if status == Task.STATE_FAILED:
            status_msg += "(%s)"%(self.getError())
        return "%s %s %0.2f hours"%(self.getTaskName(),status_msg,self.getDurationHours())

    STATE_NEW = "NEW"
    STATE_RUNNING = "RUNNING"
    STATE_COMPLETED = "COMPLETED"
    STATE_FAILED = "FAILED"

    @staticmethod
    def getAllStates():
        return [Task.STATE_NEW,Task.STATE_RUNNING,Task.STATE_COMPLETED,Task.STATE_FAILED]