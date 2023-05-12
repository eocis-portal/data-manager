# -*- coding: utf-8 -*-

#    EOCIS data-manager
#    Copyright (C) 2022-2023  National Centre for Earth Observation (NCEO)
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
The store module provides a persistent data store imlemented by python's built in SQLlite database

All database updates are performed using transactions to attempt to prevent the database from getting into an inconsistent state if
the service crashes.  This should ensure that no jobs get "lost" once a user has been notified that they have been submitted.
"""

import logging

from sqlite3 import connect
import datetime

class Store:
    """
    Implement a persistent store and methods to add and manipulate data_bundles, datasets, variables, jobs and tasks

    Tables:

    metadata:
        schema - stores the database schema version used in this database
        encrypted_schema - stores the encrypted schema

    data_bundles:
        bundle_id - a unique ID for each bundle
        bundle_name - the name of the bundle
        spec - JSON encoded spec for bundle
        minx, miny, maxx, maxy - extent in the relevant coordinate system

    datasets:
        dataset_id - a unique ID for each dataset
        bundle_id - the name of the bundle to whcih this dataset belongs
        dataset_name - the name of the dataset
        temporal_resolution - the resolution in time, eg "daily", "monthly"
        spatial_resolution - the resolution in degrees, eg 0.05
        start_date - the start date of this dataset
        end_date - the end date of this dataset
        location - the location of this bundle's files
        spec - JSON encoded spec for dataset

    variables:
        variable_id - a unique ID for variable
        variable_name - name of the variable
        dataset_id - the dataset to which this variable belongs
        spec - JSON encoded spec for variable

    jobs:
        job_id - a unique UUID generated for each job
        submission_date - the timestamp at which the job was submitted
        submitter_id - a unique reference to a submitter
        spec - the job specification as a JSON encoded string
        state - the job state (NEW, RUNNING, COMPLETED, FAILED)
        completion_date - the timestamp at which the job was completed
        error - string describing why the job failed

    tasks:
        parent_job_id - the id of the parent job to which this task belongs
        task_name - a unique name for the task within the parent job
        submission_date - the timestamp at which the task was submitted
        remote_task_id - a unique identifier for the task from the system executing the task
        spec - the task specification as a JSON encoded string
        state - the task state (NEW, RUNNING, COMPLETED, FAILED)
        completion_date - the timestamp at which the task was completed
        error - set to a non-empty error string if the task failed
        retrycount - number of attempts made to retry a failed job
    """

    SCHEMA = "V1"  # version of the database schema

    def __init__(self, path):
        """
        Implement a persistent store based on an SQLite3 database
        :param path: the path to the database file


        Initialises the store.  Will attempt to create table(s) if they do not already exist (when called for the first time)
        """
        self.path = path
        conn = connect(self.path, timeout=10.0)  # bump the standard timeout up from 5 secs to 10 secs
        curs = conn.cursor()
        # Create tables

        curs.execute('''CREATE TABLE IF NOT EXISTS bundles(
                        bundle_id text,
                        bundle_name text, 
                        spec text,
                        minx real,
                        miny real,
                        maxx real,
                        maxy real,
                        PRIMARY KEY(bundle_id));''')

        curs.execute('''CREATE TABLE IF NOT EXISTS datasets(
                        dataset_id text,
                        dataset_name text, 
                        temporal_resolution text,
                        spatial_resolution text,
                        start_date text,
                        end_date text,
                        location text,
                        spec text,
                        PRIMARY KEY(dataset_id));''')

        curs.execute('''CREATE TABLE IF NOT EXISTS dataset_bundle(
                                bundle_id text,
                                dataset_id text,
                                PRIMARY KEY(bundle_id,dataset_id),
                                FOREIGN KEY(bundle_id) REFERENCES bundles(bundle_id) ON DELETE CASCADE,
                                FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE);''')

        curs.execute('''CREATE TABLE IF NOT EXISTS variables(
                        variable_id text,
                        dataset_id text,
                        variable_name text, 
                        spec text,
                        PRIMARY KEY(variable_id,dataset_id),
                        FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE);''')

        curs.execute('''CREATE TABLE IF NOT EXISTS jobs(
                job_id text PRIMARY_KEY,
                submission_date text, 
                submitter_id text, 
                spec text, 
                state text, 
                completion_date text, 
                error text,
                PRIMARY KEY(job_id));''')

        curs.execute('''CREATE TABLE IF NOT EXISTS tasks(
                parent_job_id text, 
                task_name text, 
                submission_date text, 
                remote_task_id text, 
                spec text, 
                state text, 
                completion_date text, 
                error text, 
                retry_count int, 
                PRIMARY KEY(parent_job_id, task_name),
                FOREIGN KEY(parent_job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );''')

        # the metadata table holds the schema string and creation date
        # the schema is useful to guard against opening a database created by a different version of the software

        curs.execute('''CREATE TABLE IF NOT EXISTS metadata(
                schema text,
                creation_date text
                );''')

        # if the metadata table is empty, populate it with a single row

        curs.execute('''INSERT INTO metadata(schema,creation_date) 
                SELECT ?, ? 
                WHERE NOT EXISTS(SELECT 1 FROM metadata);''', (Store.SCHEMA, Store.encodeDate(datetime.datetime.now())))

        # check the metadata is consistent, raise an exception if not
        self.checkMetadata(conn)

        conn.commit()
        self.logger = logging.getLogger("Store")

    def checkMetadata(self, conn):
        curs = conn.cursor()
        curs.execute("SELECT schema, creation_date FROM metadata")
        results = curs.fetchall()
        if len(results) != 1:
            raise Exception("Database metadata is corrupted")

        (schema, creation_date) = tuple(results[0])

        # check that the database schema matches the schema expected by the software
        if schema != Store.SCHEMA:
            raise Exception("Unable to open database.  Database schema %s is different to current version %s." % (
            schema, Store.SCHEMA))

    def openTransaction(self):
        return Transaction(self)

    TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S"
    DATE_FORMAT = "%Y/%m/%d"

    # attribute names
    JOB_JOB_ID = "job_id"
    JOB_SUBMISSION_DATE = "submission_date"
    JOB_SUBMITTER_ID = "submitter_id"
    JOB_SPEC = "spec"
    JOB_STATE = "state"
    JOB_COMPLETION_DATE = "completion_date"
    JOB_ERROR = "error"

    TASK_PARENT_JOB_ID = "parent_job_id"
    TASK_TASK_NAME = "task_name"
    TASK_SUBMISSION_DATE = "submission_date"
    TASK_REMOTE_TASK_ID = "remote_task_id"
    TASK_SPEC = "spec"
    TASK_STATE = "state"
    TASK_COMPLETION_DATE = "completion_date"
    TASK_ERROR = "error"
    TASK_RETRY_COUNT = "retry_count"

    @staticmethod
    def encodeDate(dt):
        """Encode a datetime object as a string, compatible with Store.decodeDate"""
        if dt is None:
            return ""
        else:
            return datetime.datetime.strftime(dt, Store.DATE_FORMAT)

    @staticmethod
    def decodeDate(s):
        """Decode a string to a datetime object, compatible with Store.encodeDate"""
        if s == "" or s is None:
            return None
        else:
            return datetime.datetime.strptime(s, Store.DATE_FORMAT).replace(tzinfo=None).date()

    @staticmethod
    def encodeDateTime(dt):
        """Encode a datetime object as a string, compatible with Store.decodeDateTime"""
        if dt is None:
            return ""
        else:
            return datetime.datetime.strftime(dt, Store.TIMESTAMP_FORMAT)

    @staticmethod
    def decodeDateTime(s):
        """Decode a string to a datetime object, compatible with Store.encodeDateTime"""
        if s == "" or s is None:
            return None
        else:
            return datetime.datetime.strptime(s, Store.TIMESTAMP_FORMAT).replace(tzinfo=None)

    @staticmethod
    def renderValueList(values):
        return ",".join(map(lambda x: "'" + x + "'", values))


class Transaction(object):

    def __init__(self, store):
        self.store = store
        self.conn = connect(self.store.path)
        # this database relies on foreign keys to cascade the delete of child tasks when a parent task is deleted
        self.conn.execute("PRAGMA foreign_keys = ON;")

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def close(self):
        self.conn.commit()

    def collectResults(self, curs):
        rows = []
        column_names = [column[0] for column in curs.description]
        for row in curs.fetchall():
            rows.append({v1: v2 for (v1, v2) in zip(column_names, row)})
        return rows

