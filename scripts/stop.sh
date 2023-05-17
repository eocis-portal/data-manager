#/bin/bash

. `dirname $0`/settings.sh

pg_ctl -D $DATABASE_PATH -l $DATABASE_PATH/log.log stop
