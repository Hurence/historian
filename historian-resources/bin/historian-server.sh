#!/usr/bin/env bash



TMP="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
declare -r SCRIPT_DIR="$TMP"
ROOT_DIR="$TMP/.."
PID=$ROOT_DIR/app.pid
LOG=$ROOT_DIR/app.log
ERROR=$ROOT_DIR/app-error.log
DEBUG_MODE=false

cd "$SCRIPT_DIR" || exit
source historian.properties

USR=user

print_usage(){
    cat << EOF

bash historian-server.sh [options]

[options]:

             start
             stop
             restart
             status

-d|--debug if you want debug level logs.

by Hurence, 09/01/2019

The script start or stop the historian server.

EOF
}

# Parse method is not used for now but we put one example here if we want to add arguments
parse_args() {
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

    if [[ $DEBUG_MODE = true ]]
    then
      echo "parsing command line args"
      echo "DEBUG_MODE is set to '${DEBUG_MODE}'";
      echo "SCRIPT_DIR is $SCRIPT_DIR"
      echo "ROOT_DIR is $ROOT_DIR"
    fi

    set -- "${POSITIONAL[@]}" # restore positional parameters

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

start_historian() {
  echo "==== Start"
  touch "$PID"
#       Using an array to stock command hinder double interpretation of args
#       see https://unix.stackexchange.com/questions/444946/how-can-we-run-a-command-stored-in-a-variable for more info
  COMMAND=(java)
  if [[ $DEBUG_MODE = true ]]
  then
    COMMAND+=("-Dlog4j.configuration=file:$ROOT_DIR/conf/log4j-debug.properties")
    COMMAND+=("-Dlog4j.configurationFile=file:$ROOT_DIR/conf/log4j-debug.properties")
  else
    COMMAND+=("-Dlog4j.configuration=file:$ROOT_DIR/conf/log4j.properties")
    COMMAND+=("-Dlog4j.configurationFile=file:$ROOT_DIR/conf/log4j.properties")
  fi
  COMMAND+=(-jar "$ROOT_DIR/lib/historian-server-${HISTORIAN_VERSION}-fat.jar" --conf "$ROOT_DIR/conf/historian-server-conf.json")
  if [[ $DEBUG_MODE = true ]]
  then
    echo "run below command"
    echo "nohup ${COMMAND[@]} >> ${LOG} 2>&1 &"
  fi
  if nohup "${COMMAND[@]}" >> "$LOG" 2>&1 &
  then echo $! > "$PID"
       echo -e "${GREEN}Started.${NOCOLOR}"
       echo -e "${GREEN}check logs at '$LOG'${NOCOLOR}"
       echo "$(date '+%Y-%m-%d %X'): START" >> "$LOG"
  else echo "Error... "
       /bin/rm "$PID"
  fi
}

start() {
    if [ -f "$PID" ]
    then
#                        check if really exist
        pid_in_file=$(cat $PID )
        kill -0 $pid_in_file
        if [[ $? = 0 ]];then
#          find historian-server-processes
          historian_process=$(ps u --pid $pid_in_file)
          if [[ "$historian_process" == *historian-server-1.3.5-fat.jar* ]];then
            echo
            echo "Already started. PID: [$( cat $PID )]"
          else
            echo "cleaning not empty PID file. (processus not running anymore)."
            rm $PID
            start_historian
          fi
        else
          echo "cleaning not empty PID file. (processus not running anymore)."
          rm $PID
          start_historian
        fi
    else
        start_historian
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
        rm $PID
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



