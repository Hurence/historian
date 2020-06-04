#!/usr/bin/env bash



DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "DIR is $DIR"
ROOT_DIR="$DIR/.."
BASE="$ROOT_DIR"
echo "BASE is $BASE"
PID=$BASE/app.pid
LOG=$BASE/app.log
ERROR=$BASE/app-error.log
DEBUG_MODE=false

USR=user

print_usage(){
    cat << EOF
    historian-server.sh [options]

    -d|--debug if you want debug level logs.

    by Hurence, 09/01/2019

    The script start or stop the historian server.

EOF
}

# Parse method is not used for now but we put one exemple here if we want to add arguments
parse_args() {
    echo "parsing command line args"
    POSITIONAL=()
    while [[ $# -gt 0 ]]
    do
        key="$1"

        case $key in
            -d|--debug)
                DEBUG_MODE=true
                shift # past argument
            ;;
            -h|--help)
                print_usage
                exit 0
            ;;
            *)  # unknown option
                POSITIONAL+=("$1") # save it in an array for later
                shift # past argument
            ;;
        esac
    done

    set -- "${POSITIONAL[@]}" # restore positional parameters
    echo "DEBUG_MODE is set to '${DEBUG_MODE}'";
}

status() {
    echo
    echo "==== Status"

    if [ -f $PID ]
    then
        echo
        echo "Pid file: $( cat $PID ) [$PID]"
        echo
        ps -ef | grep -v grep | grep $( cat $PID )
    else
        echo
        echo "No Pid file"
    fi
}

start() {
    if [ -f "$PID" ]
    then
        echo
        echo "Already started. PID: [$( cat $PID )]"
    else
        echo "==== Start"
        touch "$PID"
#       Using an array to stock command hinder double interpretation of args
#       see https://unix.stackexchange.com/questions/444946/how-can-we-run-a-command-stored-in-a-variable for more info
        COMMAND=(java)
        if [ $DEBUG_MODE ]
        then
          COMMAND+=("-Dlog4j.configuration=file:./conf/log4j.properties")
          COMMAND+=("-Dlog4j.configurationFile=file:./conf/log4j.properties")
        else
          COMMAND+=("-Dlog4j.configuration=file:./conf/log4j-debug.properties")
          COMMAND+=("-Dlog4j.configurationFile=file:./conf/log4j-debug.properties")
        fi
        COMMAND+=(-jar lib/historian-server-*.jar -conf conf/historian-server-conf.json)
        echo "run below command"
        echo "nohup ${COMMAND[@]} >> ${LOG} 2>&1 &"
        if nohup "${COMMAND[@]}" >> "$LOG" 2>&1 &
        then echo $! > "$PID"
             echo -e "${GREEN}Started. ${NOCOLOR}"
             echo "$(date '+%Y-%m-%d %X'): START" >> "$LOG"
        else echo "Error... "
             /bin/rm "$PID"
        fi
    fi
}

kill_cmd() {
    SIGNAL=""; MSG="Killing "
    while true
    do
        LIST=`ps -ef | grep -v grep | grep $CMD | grep -w $USR | awk '{print $2}'`
        if [ "$LIST" ]
        then
            echo; echo "$MSG $LIST" ; echo
            echo $LIST | xargs kill $SIGNAL
            sleep 2
            SIGNAL="-9" ; MSG="Killing $SIGNAL"
            if [ -f $PID ]
            then
                /bin/rm $PID
            fi
        else
           echo; echo "All killed..." ; echo
           break
        fi
    done
}

stop() {
    echo "==== Stop"

    if [ -f $PID ]
    then
        if kill $( cat $PID )
        then echo -e "${GREEN}Stopped. ${NOCOLOR}"
             echo "$(date '+%Y-%m-%d %X'): STOP" >>$LOG
        fi
        /bin/rm $PID
        kill_cmd
    else
        echo "No pid file. Already stopped?"
    fi
}

main() {
  parse_args "$@"
  case "$1" in
    'start')
            start
            ;;
    'stop')
            stop
            ;;
    'restart')
            stop ; echo "Sleeping..."; sleep 3 ;
            start
            ;;
    'status')
            status
            ;;
    *)
            echo
            echo "Usage: $0 { start | stop | restart | status }"
            echo
            exit 1
            ;;
  esac
}

main "$@"
exit 0