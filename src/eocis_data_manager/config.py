# -*- coding: utf-8 -*-

#    sst-services
#    Copyright (C) 2020-2022  National Centre for Earth Observation (NCEO)
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

"""Defines a service configuration useful for service development and debugging on a developer's laptop"""

from os import environ

class Config:

    # web service configuration
    HOST="127.0.0.1"                    # the host name or IP address that the web service will listen on
    PORT=50001                          # the port that the web service will listen on
    DATA_URL_PREFIX = "http://127.0.0.1:50001/data"  # URL prefix for the links to output files

    # database
    DATABASE_PATH="dbname=eocis user=eocis"     # the path to the jobs database (a sqlite database file)

    # monitor
    TASK_QUOTA=1                        # the number of regridding tasks that can run in parallel
    JOB_QUOTA=2                         # the number of regridding jobs that can run in parallel
    CLEANUP_AFTER_SECS=100000           # the interval after which a job is cleaned up and its files are deleted
    MAX_TASK_RETRIES = 1                # how many times a failed task can be retried

    START_YEAR = 2022
    START_MONTH = 1
    END_YEAR = 2022
    END_MONTH = 12

    DEFAULT_START_YEAR = 2022
    DEFAULT_START_MONTH = 1

    DEFAULT_END_YEAR = 2022
    DEFAULT_END_MONTH = 12

    # output file location
    OUTPUT_PATH = "/home/dev/joboutput"  # the path to the location to store job output files

