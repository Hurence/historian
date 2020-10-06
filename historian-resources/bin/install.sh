#!/usr/bin/env bash


print_usage(){
    cat << EOF

bash historian-server.sh [options]

[options]:

    -l|--local-test <path_to_historian_dir> => Install the historian with local builded historian tgz file (for dev purpose).
    -h|--help => Print this message.

by Hurence, 09/09/2020

The script install the historian server.

EOF
}

check_brew_or_install() {
  #BREW INSTALL
  brew -v 2>&1 &>/dev/null
  if [[ $? != 0 ]]; then
    echo "Brew is not already installed. Installing it"
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    brew update
  else
    echo "Brew is already installed"
  fi
}

check_wget_or_install_with_apt_get() {
  #WGET INSTALL
  wget --version &>/dev/null
  if [ $? != 0 ]; then
    echo "wget is not already installed. Installing it"
    apt update
    apt install wget
  else
    echo "Wget is already installed"
  fi
}

check_wget_or_install_with_brew() {
  #WGET INSTALL
  wget --version &>/dev/null
  if [ $? != 0 ]; then
    echo "wget is not already installed. Installing it"
    brew install wget
    brew update
  else
    echo "Wget is already installed"
  fi
}

selectWithDefault() {

  local item i=0 numItems=$#

  # Print numbered menu items, based on the arguments passed.
  for item; do         # Short for: for item in "$@"; do
    printf '%s\n' "$((++i))) $item"
  done >&2 # Print to stderr, as `select` does.

  # Prompt the user for the index of the desired item.
  while :; do
    printf %s "${PS3-#? }" >&2 # Print the prompt string to stderr, as `select` does.
    read -r index
    # Make sure that the input is either empty or that a valid index was entered.
    [[ -z $index ]] && break  # empty input
    (( index >= 1 && index <= numItems )) 2>/dev/null || { echo "Please choose 1 or 2" >&2; continue; }
    break
  done

  # Output the selected item, if any.
  [[ -n $index ]] && printf %s "${@: index:1}"
}

setup_all_variables() {
  local MSG="Where do you want to install Hurence Data Historian ?"
  ask_and_set_variable "HDH_HOME" "/opt/hdh" "$MSG"
  if [[ $HDH_HOME == "~"* ]]; then
    HDH_HOME_WITHOUT_START_TILT="${HDH_HOME:1}"
    HDH_HOME="$(echo ~)$HDH_HOME_WITHOUT_START_TILT"
  fi
  echo "Will install historian in ${HDH_HOME}"
  MSG="Do you want us to install an embedded solr (version 8.2.0 required)? (otherwise you need to have one that can be used from this machine) [Yes] "
  ask_and_set_boolean_variable "USING_EMBEDDED_SOLR" "$MSG"
  if [[ $USING_EMBEDDED_SOLR  = false ]]; then
    MSG="What is the path to the solr cluster ? We will use the solr REST api to create collection."
    ask_and_set_variable "SOLR_HOST_PORT_SOLR" "$DEFAULT_HOST_PORT_SOLR" "$MSG"
  else
    export SOLR_HOST_PORT_SOLR="$DEFAULT_HOST_PORT_SOLR"
  fi
  MSG="Which name to use for the solr collection which will be storing time series ?"
  ask_and_set_variable "CHUNK_COLLECTION_NAME" "historian" "$MSG"
  MSG="Which name to use for the solr report collection ?"
  ask_and_set_variable "REPORT_COLLECTION_NAME" "historian-report" "$MSG"
  MSG="Do you want to add tags names for your time series (you can always add them after installation )? [No]"
  ask_and_update_array "TAG_NAMES" "$MSG" "Tag name"
  if [ $CREATE = true ]; then
  MSG="Do you confirm the creation of these tags? [Yes]"
  confirmation_creation_array "RESET_TAG" "$MSG"
  fi
  while [[ $RESET_TAG = false ]]; do
    MSG="Do you want to add tags names for your time series (you can always add them after installation )? [No]"
    ask_and_update_array "TAG_NAMES" "$MSG" "Tag name"
    MSG="Do you confirm the creation of these tags? [Yes]"
    confirmation_creation_array "RESET_TAG" "$MSG"
  done
  MSG="Do you want us to install an embedded grafana (version 7.0.3 required)? (otherwise you need to have one that can be used from this machine if you plan to use grafana) [Yes]"
  ask_and_set_boolean_variable "USING_EMBEDDED_GRAFANA" "$MSG"
  if [[ $USING_EMBEDDED_GRAFANA = true ]]; then
    export GRAFANA_HOME="$HDH_HOME/grafana-7.0.3"
  fi
  MSG="Do you want us to install the historian datasource grafana plugin ? You need it to see data with grafana. [Yes]"
  MSG="$MSG We can install it only if grafana is on this machin as single node otherwise you will have to install it manually."
  MSG="$MSG If you choose to install an embedded grafana you can install it as well."
  ask_and_set_boolean_variable "INSTALLING_DATASOURCE_PLUGIN" "$MSG"
  if [[ $INSTALLING_DATASOURCE_PLUGIN = true ]]; then
    if [[ -v GRAFANA_HOME ]]; then
      export GRAFANA_PLUGIN_DIR="$GRAFANA_HOME/data/plugins"
    else
      MSG="What is the path to your grafana installation plugin folder ? "
      ask_and_set_variable "GRAFANA_PLUGIN_DIR" "path/to/grafana/data/plugins"
    fi
  fi
  MSG="Do you want us to install an embedded spark (this is not required)? [Yes]"
  ask_and_set_boolean_variable "INSTALLING_SPARK" "$MSG"
  #setup variable from others
  export HISTORIAN_HOME="$HDH_HOME/$HISTORIAN_DIR_NAME"
  export SOLR_CLUSTER_HISTORIAN_CHUNK_URL="http://$SOLR_HOST_PORT_SOLR/$CHUNK_COLLECTION_NAME"
}

print_conf() {
  echo "will start the install with those parameters :"
  echo "HDH_HOME : $HDH_HOME"
  echo "USING_EMBEDDED_SOLR : $USING_EMBEDDED_SOLR"
  echo "SOLR_HOST_PORT_SOLR : $SOLR_HOST_PORT_SOLR"
  echo "CHUNK_COLLECTION_NAME : $CHUNK_COLLECTION_NAME"
  echo "REPORT_COLLECTION_NAME : $REPORT_COLLECTION_NAME"
  echo "TAG_NAMES to add for time series : $(join_by , "${TAG_NAMES[@]}")"
  echo "USING_EMBEDDED_GRAFANA : $USING_EMBEDDED_GRAFANA"
  echo "INSTALLING_DATASOURCE_PLUGIN : $INSTALLING_DATASOURCE_PLUGIN"
  echo "GRAFANA_PLUGIN_DIR : $GRAFANA_PLUGIN_DIR"
  echo "GRAFANA_HOME : $GRAFANA_HOME"
  echo "INSTALLING_SPARK : $INSTALLING_SPARK"
  echo -e "\n"
}

#ask user to enter value for the variable, if user just press enter the default value is used instead
#param1 Variable name to set
#param2 Default value for the variable
#param3 Msg description
ask_and_set_variable() {
  local MSG="$3"
  local variable_name_to_modify="$1"
  local -n variable_value="${variable_name_to_modify}"
  local DEFAULT_VALUE="$2"
  echo "${MSG}[$DEFAULT_VALUE]"
  read -r "${variable_name_to_modify?}"
  export "${variable_name_to_modify}=${variable_value:-$DEFAULT_VALUE}"
}

#ask user to enter value for the variable, if user just press enter the default value is used instead
#param1 Variable name to set
#param2 Default value for the variable
#param3 Msg description
ask_and_set_variable_should_be_absolute_path() {
  local MSG="$3"
  local variable_name_to_modify="$1"
  local -n variable_value="${variable_name_to_modify}"
  local DEFAULT_VALUE="$2"
  echo "${MSG}[$DEFAULT_VALUE]"
  read -r "${variable_name_to_modify?}"
  export "${variable_name_to_modify}=${variable_value:-$DEFAULT_VALUE}"
  if [[ ! ${variable_value:-$DEFAULT_VALUE} = /* ]]; then
        echo "Not an acceptable value, please specify an absolute path."
        ask_and_set_variable_should_be_absolute_path "$1" "$2" "$3"
  fi 
}

join_by() { 
  local d=$1; 
  shift; 
  local f=$1; 
  shift; 
  printf %s "$f" "${@/#/$d}"; 
  echo
}
#Similar to ask_and_set_variable but the input variable is an array and user can enter several value
#The variable array must previusly exist ! Indeed it is not possible to export an array variable
#param1 Variable name to set
#param2 Msg description
#param3 Msg description for element
ask_and_update_array() {
  local variable_name_to_modify="$1"
  local -n _array="${variable_name_to_modify}"
  local MSG="$2"
  local MSG2="$3"
  unset tag_name
  echo "$MSG"
  optionsAudits=('Yes' 'No')
  select_tag=$(selectWithDefault "${optionsAudits[@]}")
  case $select_tag in
    'Yes') while [ -z "$tag_name" ] || ([ "$tag_name" != 'STOP' ] && [ "$tag_name" != 'stop' ]);
      do
        read -p "${MSG2} (STOP when you want stop): " tag_name
        if [ "$tag_name" != 'STOP' ] && [ "$tag_name" != '' ] && [ "$tag_name" != 'stop' ]; then
          echo "tapped tag $tag_name"
          _array+=("$tag_name")
        fi
      done
      echo "Tags : [$(join_by , "${_array[@]}")]"

      CREATE=true
      ;;

    ''|'No') CREATE=false;;
  esac
}

#ask user to confirm the creation of array of ask_and_update_array
#if user just press enter the default value is used instead
#param1 Variable name to set
#param2 Msg description
confirmation_creation_array() {
local variable_name_to_modify="$1"
local MSG="$2"
echo "$MSG"
  optionsAudits=('Yes' 'No')
  select_reset_tag=$(selectWithDefault "${optionsAudits[@]}")
    case $select_reset_tag in
     ''|'Yes') export "${variable_name_to_modify}=true"
               ;;
     'No') unset TAG_NAMES[@]
           export "${variable_name_to_modify}=false"
           ;;
    esac
}

#ask user to enter value for the variable, if user just press enter the default value is used instead
#the variable is a boolean (empty string or not)
#param1 Variable name to set
#param2 Msg description
ask_and_set_boolean_variable() {
  local variable_name_to_modify="$1"
  local MSG="$2"
  echo "$MSG"
  optionsAudits=('Yes' 'No')
  select_solr=$(selectWithDefault "${optionsAudits[@]}")
  case $select_solr in
  ''|'Yes')
         export "${variable_name_to_modify}=true"
         ;;

  'No')
         export "${variable_name_to_modify}=false"
         ;; # $opt is '' if the user just pressed ENTER
  esac
}

add_tag_names_to_chunk_collection() {
  for tag in "${TAG_NAMES[@]}"; do
    $HISTORIAN_HOME/bin/modify-collection-schema.sh -c "$CHUNK_COLLECTION_NAME" -s "$SOLR_HOST_PORT_SOLR" -f "$tag"
    echo -e "\n"
  done
}

download_and_extract_historian_into_hdh_home() {
  #Historian download & extract
  if [[ $DEBUG_MODE = true ]]
  then
    mkdir -p "$HDH_HOME" && tar -xf ${LOCAL_HISTORIAN_TGZ} -C "$HDH_HOME"
  else
    wget "${URL_HISTORIAN_TGZ_RELEASE}"
    mkdir -p "$HDH_HOME" && tar -xf ${HISTORIAN_TGZ_NAME} -C "$HDH_HOME"
    rm "${HISTORIAN_TGZ_NAME}"
  fi
  echo "installed historian into $HDH_HOME"
}

#Install embeded solr if needed and setup SOLR_HOST_PORT_SOLR, default or user input
install_embedded_solr_and_start_it_if_needed() {
  if [[ $USING_EMBEDDED_SOLR = true ]]; then
    # create 2 data folders for SolR data
    local -r SOLR_NODE_1="$HDH_HOME/data/solr/node1"
    local -r SOLR_NODE_2="$HDH_HOME/data/solr/node2"
    mkdir -p $SOLR_NODE_1 $SOLR_NODE_2
    # touch a few config files for SolR
    local -r SOLR_XML_PATH="$HISTORIAN_HOME/conf/solr.xml"
    cp ${SOLR_XML_PATH} ${SOLR_NODE_1}/solr.xml
    cp ${SOLR_XML_PATH} ${SOLR_NODE_2}/solr.xml
    touch ${HDH_HOME}/data/solr/node1/zoo.cfg
    # get SolR 8.2.0 and unpack it
    wget https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz
    tar -xf solr-8.2.0.tgz
    rm solr-8.2.0.tgz
    # start a SolR cluster locally with an embedded zookeeper
    local -r SOLR_HOME="$HDH_HOME/solr-8.2.0"
    # démarre un core Solr localement ainsi qu'un serveur zookeeper standalone.

    # We use force to enable solr to be run even as root user.
    ${SOLR_HOME}/bin/solr start -cloud -s "$SOLR_NODE_1" -p 8983 -force
    # démarre un second core Solr localement qui va utiliser le serveur zookeeper précédament créer.
    # We use force to enable solr to be run even as root user.
    ${SOLR_HOME}/bin/solr start -cloud -s "$SOLR_NODE_2" -p 7574 -z localhost:9983 -force
    echo "solr is now running at ${SOLR_HOST_PORT_SOLR}"
    echo -e "\n"
  fi
}

intall_grafana_if_asked_for_mac() {
  if [[ $USING_EMBEDDED_GRAFANA = true ]]; then
    wget https://dl.grafana.com/oss/release/grafana-7.0.3.darwin-amd64.tar.gz
    tar -zxf grafana-7.0.3.darwin-amd64.tar.gz
    rm grafana-7.0.3.darwin-amd64.tar.gz
  fi
  echo -e "\n"
}

intall_grafana_if_asked_for_linux() {
  if [[ $USING_EMBEDDED_GRAFANA = true ]]; then
    wget https://dl.grafana.com/oss/release/grafana-7.0.3.linux-amd64.tar.gz
    tar -zxf grafana-7.0.3.linux-amd64.tar.gz
    rm grafana-7.0.3.linux-amd64.tar.gz
  fi
  echo -e "\n"
}

intall_grafana_datasource_plugin_if_asked() {
  if [[ $INSTALLING_DATASOURCE_PLUGIN = true ]]; then
    wget https://github.com/Hurence/grafana-historian-datasource/releases/download/v1.0.0/historian-datasource-plugin.tgz
    mkdir -p $GRAFANA_PLUGIN_DIR
    tar -zxf historian-datasource-plugin.tgz -C $GRAFANA_PLUGIN_DIR
    rm historian-datasource-plugin.tgz
    echo "Installed grafana plugin into $GRAFANA_PLUGIN_DIR"
    echo -e "\n"
  fi
}

start_grafana_if_asked() {
  if [[ $USING_EMBEDDED_GRAFANA = true ]]; then
    cd $GRAFANA_HOME
    nohup ./bin/grafana-server web &
    cd -
  fi
}

intall_spark_if_asked() {
  if [[ $INSTALLING_SPARK = true ]]; then
    # get Apache Spark 2.3.4 and unpack it
    wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz
    tar -xf spark-2.3.4-bin-without-hadoop.tgz
    rm spark-2.3.4-bin-without-hadoop.tgz
  fi
}

generate_historian_server_conf() {
  #Generation du fichier de configuration selon les informations renseignées ( stream_url & chunk_collection )
  echo '{
    "web.verticles.instance.number": 1,
    "historian.verticles.instance.number": 2,
    "http_server" : {
      "host": "localhost",
      "port" : 8080,
      "historian.address": "historian",
      "debug": false,
      "max_data_points_maximum_allowed" : 50000
    },
    "historian": {
      "schema_version": "VERSION_1",
      "address" : "historian",
      "limit_number_of_point_before_using_pre_agg" : 50000,
      "limit_number_of_chunks_before_using_solr_partition" : 50000,
      "api": {
        "grafana": {
          "search" : {
            "default_size": 100
          }
        }
      },
      "solr" : {
        "use_zookeeper": true,
        "zookeeper_urls": ["localhost:9983"],
        "zookeeper_chroot" : null,
        "stream_url" : "'${SOLR_CLUSTER_HISTORIAN_CHUNK_URL}'",
        "chunk_collection": "'${CHUNK_COLLECTION_NAME}'",
        "annotation_collection": "annotation",
        "sleep_milli_between_connection_attempt" : 10000,
        "number_of_connection_attempt" : 3,
        "urls" : null,
        "connection_timeout" : 10000,
        "socket_timeout": 60000
      }
    }
  }' > $HISTORIAN_HOME/conf/historian-server-conf.json
}

create_historian_collections() {
  # create collection in SolR
  $HISTORIAN_HOME/bin/create-historian-chunk-collection.sh -c "$CHUNK_COLLECTION_NAME" -s "$SOLR_HOST_PORT_SOLR"
  echo -e "\n"
  # create report collection in SolR
  $HISTORIAN_HOME/bin/create-historian-report-collection.sh -c "$REPORT_COLLECTION_NAME" -s "$SOLR_HOST_PORT_SOLR"
  echo -e "\n"
}

start_historian_server() {
  # and launch the historian REST server
  echo "Install completed. Starting historian..."
  $HISTORIAN_HOME/bin/historian-server.sh start
  echo "The historian server is now running."
  echo "You can use ${HISTORIAN_HOME}/bin/historian-server.sh [start|stop|restart] to manage the historian server."
}

go_to_home_directory() {
  echo "HDH_HOME  is $HDH_HOME"
  cd ${HDH_HOME} || (echo "could not go to ${HDH_HOME} folder" && exit 1)
  echo "directory of installation is $(pwd)"
}


create_home_directory() {
  mkdir -p ${HDH_HOME}
  CREATION_OF_HDH_HOME_RESULT=$?
  if [[ $CREATION_OF_HDH_HOME_RESULT != 0 ]]; then
    echo "could not create $HDH_HOME folder ! returned $CREATION_OF_HDH_HOME_RESULT"
    exit $CREATION_OF_HDH_HOME_RESULT
  fi
}



# Parse method is not used for now but we put one exemple here if we want to add arguments
parse_args() {
    POSITIONAL=()
    while [[ $# -gt 0 ]]
    do
        key="$1"
        case $key in
            -l|--local-test)
                export DEBUG_MODE=true
                export LOCAL_HISTORIAN_TGZ="$2"
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
      echo "LOCAL_HISTORIAN_TGZ is set to '${LOCAL_HISTORIAN_TGZ}'";
    fi

    set -- "${POSITIONAL[@]}" # restore positional parameters
}


main() {
  parse_args "$@"
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
      echo "Os detected is linux : $OSTYPE"
      check_wget_or_install_with_apt_get
      #Setup conf with user (need to declare arrays in advance)
      declare -a TAG_NAMES=()
      setup_all_variables
      print_conf
      create_home_directory
      go_to_home_directory
      download_and_extract_historian_into_hdh_home
      install_embedded_solr_and_start_it_if_needed
      create_historian_collections
      add_tag_names_to_chunk_collection
      intall_grafana_if_asked_for_linux
      intall_grafana_datasource_plugin_if_asked
      start_grafana_if_asked
      intall_spark_if_asked
      generate_historian_server_conf
      start_historian_server
  elif [[ "$OSTYPE" == "darwin"* ]]; then #Mac OS
      echo "Os detected is Mac os : $OSTYPE"
      check_brew_or_install
      check_wget_or_install_with_brew
      #Setup conf with user (need to declare arrays in advance)
      declare -a TAG_NAMES=()
      setup_all_variables
      print_conf
      create_home_directory
      go_to_home_directory
      download_and_extract_historian_into_hdh_home
      install_embedded_solr_and_start_it_if_needed
      create_historian_collections
      add_tag_names_to_chunk_collection
      intall_grafana_if_asked_for_mac
      intall_grafana_datasource_plugin_if_asked
      start_grafana_if_asked
      intall_spark_if_asked
      generate_historian_server_conf
      start_historian_server
  elif [[ "$OSTYPE" == "cygwin" ]]; then
      echo "this os is not yet supported : $OSTYPE"
      exit 1
  elif [[ "$OSTYPE" == "msys" ]]; then
      echo "this os is not yet supported : $OSTYPE"
      exit 1
  elif [[ "$OSTYPE" == "win32" ]]; then
      echo "this os is not yet supported : $OSTYPE"
      exit 1
  elif [[ "$OSTYPE" == "freebsd"* ]]; then
      echo "this os is not yet supported : $OSTYPE"
      exit 1
  else
      echo "this os is not yet supported : $OSTYPE"
      exit 1
  fi
  exit 0
}

declare -r DEFAULT_HOST_PORT_SOLR="localhost:8983/solr"
declare -r HISTORIAN_VERSION="1.3.6-SNAPSHOT"
declare -r HISTORIAN_DIR_NAME="historian-${HISTORIAN_VERSION}"
declare -r HISTORIAN_TGZ_NAME="${HISTORIAN_DIR_NAME}-bin.tgz"
declare -r URL_HISTORIAN_TGZ_RELEASE="https://github.com/Hurence/historian/releases/download/v${HISTORIAN_VERSION}/${HISTORIAN_TGZ_NAME}"

main "$@"

