#!/bin/bash
### BEGIN INIT INFO
# Provides:           someapp
# Required-Start:     $remote_fs $network
# Required-Stop:      $remote_fs $network
# Default-Start:      2 3 4 5
# Default-Stop:       0 1 6
# Short-Description:  Start someapp
### END INIT INFO
#
# make a link to this file from etc/init.d;
# ln -s <this file> /etc/init.d/
#
# create the runlevel-links like this;
# sudo update-rc.d someapp defaults 50
#
# now this should work;
# invoke-rc.d someapp start
# invoke-rc.d someapp stop

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

# (hopefully) self-explanatory config parameters
# it should not be necessary to change anything outside of this block
export ITEM=rets
export ITEM_STARTMOD=$ITEM
export ITEM_USER=$ITEM
export ITEM_LOGDIR=`tryt /var/log /tmp`/$ITEM
export ITEM_LIBDIR=`eval echo ~$ITEM_USER`/git/$ITEM
export ITEM_DEPS="$ITEM_LIBDIR/deps/*"
export ITEM_ERL=`which erl`
export ITEM_GROUP=`id -g $ITEM_USER`
export ITEM_BOOTLOG=$ITEM_LOGDIR/boot.log
export ITEM_ERLLOG=$ITEM_LOGDIR/erlang.log
export ITEM_SNAME=`echo $ITEM | tr "\-." "_"`

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
    sudo -u $ITEM_USER \
        $ITEM_ERL  \
        -sname $ITEM_SNAME \
        -setcookie $ITEM \
        -boot start_sasl \
        -kernel error_logger "{file,\"$ITEM_ERLLOG\"}" \
        -pa $ITEM_LIBDIR/ebin \
        $PAS \
        -run $ITEM_STARTMOD \
        -detached
}

item_stop() {
    P=`beamgrep $ITEM_USER "run $ITEM_STARTMOD"`
    [ -n "$P" ] && sudo -u $ITEM_USER kill -KILL $P
}

item_shell() {
    HOST=`echo $HOSTNAME | cut -f1 -d"."`
    sudo -u $ITEM_USER \
        $ITEM_ERL \
        -sname $$ -remsh ${ITEM_SNAME}@${HOST} -setcookie $ITEM
}

out() {
    flag=""
    [ "$1" == "-n" ] && flag="-n" && shift
    echo $flag "$1" >> $ITEM_BOOTLOG
}

out "`date | tr ' ' '-'` "
case "$1" in
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
        $0 stop
        sleep 1
        $0 start
        ;;
    attach | shell)
        out "attaching $USER - $$."
        item_shell || out -n "Failed... "
        out "detaching $USER - $$."
        ;;
    *)
        exit 1
esac
