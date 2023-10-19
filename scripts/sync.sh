#!/bin/bash

rootfolder=`dirname $0`/..

rsync -avr $rootfolder/src dev@eocis.org:/home/dev/services/data-manager
rsync -avr $rootfolder/schema dev@eocis.org:/home/dev/services/data-manager
rsync -avr $rootfolder/scripts dev@eocis.org:/home/dev/services/data-manager
rsync -av $rootfolder/setup.cfg dev@eocis.org:/home/dev/services/data-manager
rsync -av $rootfolder/pyproject.toml dev@eocis.org:/home/dev/services/data-manager

