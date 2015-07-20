#!/bin/bash
### BEGIN INIT INFO
# Provides:           rets
# Required-Start:     $remote_fs $network
# Required-Stop:      $remote_fs $network
# Default-Start:      2 3 4 5
# Default-Stop:       0 1 6
# Short-Description:  Start rets
### END INIT INFO
#
# make a link to this file from etc/init.d;
# ln -s <this file> /etc/init.d/
# or copy
# cp <this file> /etc/init.d/
#
# create the runlevel-links like this;
# sudo update-rc.d rets defaults 50
#
# now this should work;
# invoke-rc.d rets start
# invoke-rc.d rets stop

# $1 is a list of dirs. return the first dir that is writable.
tryt() {
    while [ 0 -lt $# ]; do
        if 2>/dev/null touch $1/$$ && rm $1/$$; then
            echo $1
            return
        else
            shift
        fi
    done
    exit -1
}

die() {
    echo "$1"
    exit 1
}

usage() {
    echo "$0 [options]"
    echo "-b database backend"
    echo "-d data_dir"
    echo "-k keep the data base between restarts"
    echo "-l code dir (should contain ebin and deps)"
    echo "-L logdir"
    echo "-p port number"
    echo "-u user name wto run service under"
    status
}

status() {
    echo "backend:  $ITEM_BACKEND"
    echo "data dir: $ITEM_TABDIR"
    echo "keep_db:  $ITEM_KEEPDB"
    echo "libdir:   $ITEM_LIBDIR"
    echo "logdir:   $ITEM_LOGDIR"
    echo "port:     $ITEM_PORT"
    echo "user:     $ITEM_USER"
    exit 0
}

export ITEM=rets

# default value of configs
export ITEM_BACKEND=leveldb
export ITEM_PORT=7890
export ITEM_USER=$USER
export ITEM_KEEPDB=false
export ITEM_LOGDIR=`tryt /var/log /tmp`/$ITEM
export ITEM_LIBDIR=`eval echo ~$ITEM_USER`/git/$ITEM
export ITEM_TABDIR=$ITEM_LIBDIR/db

# check command line flags
while getopts ":b:d:l:L;p:u:hs" opt; do
    case "$opt" in
        b) export ITEM_BACKEND=$OPTARG;;
        d) export ITEM_TABDIR=$OPTARG;;
        l) export ITEM_LIBDIR=$OPTARG;;
        L) export ITEM_LOGDIR=$OPTARG;;
        p) export ITEM_PORT=$OPTARG;;
        u) export ITEM_USER=$OPTARG;;
        s) status;;
        h) usage;;
        ':') die "missing arg: $opt $OPTARG";;
        '?') die "bad option: $OPTARG";;
    esac
done
FLAGS="${@:1:OPTIND-1}"
ACTION="${!OPTIND}"

export ITEM_STARTMOD=$ITEM
export ITEM_DEPS="$ITEM_LIBDIR/deps/*"
export ITEM_ERL=`which erl`
export ITEM_GROUP=`id -g $ITEM_USER`
export ITEM_BOOTLOG=$ITEM_LOGDIR/boot.log
export ITEM_ERLLOG=$ITEM_LOGDIR/erlang.log
export ITEM_SNAME=`echo $ITEM | tr "\-." "_"`

if [ "$USER" == "$ITEM_USER" ]; then
    CONTEXT="eval"
else
    CONTEXT="su ${ITEM_USER} --command"
fi

[ -d $ITEM_LOGDIR ] || mkdir $ITEM_LOGDIR
chown $ITEM_USER:$ITEM_GROUP $ITEM_LOGDIR
[ -f $ITEM_BOOTLOG ] || touch $ITEM_BOOTLOG
chown $ITEM_USER:$ITEM_GROUP $ITEM_BOOTLOG

# osx does not have pkill. #fail
beamgrep() {
    ps -u "$1" -o pid,command | grep beam | grep "$2" | cut -c-6
}

item_start() {
    P=`beamgrep $ITEM_USER "run $ITEM_STARTMOD"`
    [ -n "$P" ] && return
    for dep in $ITEM_DEPS; do
        [ -d $dep/ebin ] && PAS="$PAS -pa $dep/ebin"
    done
    STARTCMD=" $ITEM_ERL \
        -sname $ITEM_SNAME \
        -setcookie $ITEM \
        -boot start_sasl \
        -kernel error_logger \"{file,\\\"$ITEM_ERLLOG\\\"}\" \
        -rets backend $ITEM_BACKEND \
        -rets table_dir \\\"$ITEM_TABDIR\\\" \
        -rets keep_db $ITEM_KEEPDB \
        -rets port_number $ITEM_PORT \
        -pa $ITEM_LIBDIR/ebin \
        $PAS \
        -run $ITEM_STARTMOD \
        -detached"
    ${CONTEXT} "${STARTCMD}"
}

item_stop() {
    P=`beamgrep $ITEM_USER "run $ITEM_STARTMOD"`
    [ -n "$P" ] && $CONTEXT "kill -KILL $P"
}

item_shell() {
    HOST=`echo $HOSTNAME | cut -f1 -d"."`
    SHLCMD="$ITEM_ERL -sname $$ -remsh ${ITEM_SNAME}@${HOST} -setcookie $ITEM"
    $CONTEXT "$SHLCMD"
}

out() {
    flag=""
    [ "$1" == "-n" ] && flag="-n" && shift
    echo $flag "$1" >> $ITEM_BOOTLOG
}

out "`date | tr ' ' '-'` "

case "$ACTION" in
    status)
        P=`beamgrep $ITEM_USER "run $ITEM_STARTMOD"`
        [ -n "$P" ] && echo $P
        ;;
    start)
        out "logging to "$ITEM_LOGDIR
        out -n "starting $0..."
        item_start && out -n " started..."
        out " done."
        ;;
    stop)
        out -n "stopping..."
        item_stop && out -n " stopped..."
        out " done."
        ;;
    restart | force-reload)
        out "restarting."
        $0 $FLAGS stop
        sleep 1
        $0 $FLAGS start
        ;;
    attach | shell)
        out "attaching $USER - $$."
        item_shell || out -n "Failed... "
        out "detaching $USER - $$."
        ;;
    *)
        echo "unrecognized command: $ACTION"
        out "unrecognized command: $ACTION"
        exit 1
esac
