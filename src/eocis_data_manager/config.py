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
    DATABASE_PATH="current_jobs.db"     # the path to the jobs database (a sqlite database file)

    # monitor
    MONITOR_INTERVAL=5                  # the interval between monitor cycles, in seconds
    TASK_QUOTA=1                        # the number of regridding tasks that can run in parallel
    JOB_QUOTA = 2                       # the number of regridding jobs that can run in parallel
    CLEANUP_AFTER_SECS=100000           # the interval after which a job is cleaned up and its files are deleted
    DISK_SPACE_CHECK_GB = 10           # set a threshold of free space (in Gb) that must exist in the output folder for any new tasks to be started
    MAX_TASK_RETRIES = 1                # how many times a failed task can be retried
    CACHE_WRITABLE = False              # set to true to cache the results of any executed job

    START_YEAR = 2014
    START_MONTH = 9
    END_YEAR = 2016
    END_MONTH = 11

    DEFAULT_START_YEAR = 2015
    DEFAULT_START_MONTH = 1

    DEFAULT_END_YEAR = 2016
    DEFAULT_END_MONTH = 11


    # define the locations of the scripts (you'll need to adjust these paths)
    REGRID_SCRIPT_PATH = "/home/dev/github/regridding_tools/global_regridding/makegriddedSSTs.py"
    TIMESERIES_SCRIPT_PATH = "/home/dev/github/regridding_tools/timeseries_region/maketimeseriesSSTs.py"
    REGION_SCRIPT_PATH = "/home/dev/github/regridding_tools/timeseries_region/makeregionSSTs.py"
    INTERPRETER_PATH = "/home/dev/miniconda3/envs/cfpy37/bin/python"

    # input file locations (you'll need to adjust these paths)
    SST_CCI_ANALYSIS_L4_PATH = "/home/dev/Data/sst/data/CDR_v2/Analysis/L4/v2.1/"
    C3S_SST_ANALYSIS_L4_PATH = "/home/dev/Data/sst/data/ICDR_v2/Analysis/L4/v2.0/"
    SST_CCI_CLIMATOLOGY_PATH = "/home/dev/Data/sst/data/CDR_v2/Climatology/L4/v2.1/"
    RESLICE_PATH = "/home/dev/data/Data/reslice/"

    # input file locations with dust correction (you'll need to adjust these paths)
    DUST_SST_CCI_ANALYSIS_L4_PATH = "/home/dev/Data/sst_dust/data/CDR_v2/Analysis/L4/v2.1/"
    DUST_C3S_SST_ANALYSIS_L4_PATH = "/home/dev/Data/sst_dust/data/ICDR_v2/Analysis/L4/v2.0/"
    DUST_SST_CCI_CLIMATOLOGY_PATH = "/home/dev/Data/sst_dust/data/CDR_v2/Climatology/L4/v2.1/"
    DUST_RESLICE_PATH = "/home/dev/Data/sst/data/reslice/"

    # output file location
    OUTPUT_PATH = "/home/dev/joboutput"  # the path to the location to store job output files

