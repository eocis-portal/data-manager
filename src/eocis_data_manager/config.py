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

class Config:

    # web service configuration
    HOST="127.0.0.1"                    # the host name or IP address that the web service will listen on
    PORT=50010                          # the port that the web service will listen on
    # DATA_URL_PREFIX = "https://eocis.org/joboutput"  # URL prefix for the links to output files
    DATA_URL_PREFIX = "/joboutput"  # URL prefix for the links to output files

    # database
    DATABASE_PATH="dbname=eocis user=eocis"     # the path to the jobs database

    # monitor
    TASK_QUOTA=1                        # the number of regridding tasks that can run in parallel
    JOB_QUOTA=2                         # the number of regridding jobs that can run in parallel
    CLEANUP_AFTER_SECS=100000           # the interval after which a job is cleaned up and its files are deleted
    MAX_TASK_RETRIES = 1                # how many times a failed task can be retried

    # output file location
    OUTPUT_PATH = "/data/data_service/joboutput"  # the path to the location to store job output files
    OUTPUT_FILENAME_PATTERN = "{Y}{m}{d}{H}{M}{S}-EOCIS-{LEVEL}-{PRODUCT}-v{VERSION}-fv01.0"

