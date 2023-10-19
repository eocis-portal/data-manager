#/bin/bash

. `dirname $0`/settings.sh

if [ ! -d "$DATABASE_PATH" ]; then
  initdb -D $DATABASE_PATH
fi

##############################

pg_ctl -D $DATABASE_PATH -l $DATABASE_PATH/log.log start

createuser --encrypted $USERNAME

createdb --owner=$USERNAME $DBNAME
