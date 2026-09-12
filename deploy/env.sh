# Shared settings for the deployment scripts. Sourced, not executed.
#
# Every value here can be overridden from the environment, so a box that is
# different in one respect does not need its own copy of the scripts.

: "${APP_DIR:=$HOME/csd-group-chat}"
: "${SERVER_DIR:=$APP_DIR/server}"
: "${LOG_DIR:=$HOME/logs}"

: "${MONGO_DBPATH:=$HOME/mongo-data}"
: "${MONGO_PORT:=27017}"
: "${MONGO_BIND:=127.0.0.1}"

# WiredTiger will size its cache from the *host's* memory if it is not told
# otherwise, and the host has 128GB while this container is allowed 512MB. That
# mismatch is what got mongod and the backend killed together: the database
# reserved a cache larger than the whole container, and the kernel chose a
# victim by the total. A quarter of a gigabyte leaves room for the backend.
: "${MONGO_CACHE_GB:=0.25}"

# How long a supervised process has to stay up before its restart delay is
# considered recovered. Below this it is crash-looping, and the delay backs off
# so that a process failing on startup does not spin the CPU it is meant to be
# serving with.
: "${RESTART_MIN_UPTIME:=20}"
: "${RESTART_DELAY_MIN:=1}"
: "${RESTART_DELAY_MAX:=30}"
