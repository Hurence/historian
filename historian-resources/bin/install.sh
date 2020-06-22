#!/usr/bin/env bash

print_usage() {
  cat <<EOF
    install.sh

    by Hurence, 09/01/2019

EOF
}

check_brew_or_install() {
  #BREW INSTALL
  if [ "$(brew 2>&1)" = "install.sh: line 21: brew: command not found" ]; then
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    brew update
  else
    echo "Brew is already installed"
  fi
}

check_wget_or_install() {
  #WGET INSTALL
  if [ "$(wget 2>&1)" = "install.sh: line 30: wget: command not found" ]; then
    brew install wget
    brew update
  else
    echo "Wget is already installed"
  fi
}

check_maven_or_install() {
  #MAVEN INSTALL
  maven_check=$(mvn 2>&1)
  if [ "$maven_check" = "install.sh: line 40: mvn: command not found" ]; then
    brew install maven
    brew update
  else
    echo "Maven is already installed"
  fi
}

setup_all_variables() {
  local MSG="Where do you want to install Hurence Data Historian ?"
  ask_and_set_variable "HDH_HOME" "/opt/hdh" "$MSG"
  MSG="Do you want us to install an embedded solr (version 8.2.0 required)? (otherwise you need to have one that can be used from this machine)"
  ask_and_set_boolean_variable "USING_EMBEDDED_SOLR" "$MSG"
  if [[ ! $USING_EMBEDDED_SOLR ]]; then
    MSG="What is the path to the solr cluster ? We will use the solr REST api to create collection."
    ask_and_set_variable "SOLR_CLUSTER_URL" "$DEFAULT_SOLR_CLUSTER_URL" "$MSG"
  else
    SOLR_CLUSTER_URL="$DEFAULT_SOLR_CLUSTER_URL"
  fi
  MSG="Which name to use for the solr collection which will be storing time series ?"
  ask_and_set_variable "CHUNK_COLLECTION_NAME" "historian" "$MSG"
  MSG="Which name to use for the solr report collection ?"
  ask_and_set_variable "REPORT_COLLECTION_NAME" "historian-report" "$MSG"
  MSG="Do you want to add tags names for your time series (you can always add them after installation ?)"
  ask_and_update_array "TAG_NAMES" "$MSG" "Tag name"
  MSG="Do you want us to install an embedded grafana (version 7.0.3 required)? (otherwise you need to have one that can be used from this machine if you plan to use grafana)"
  ask_and_set_boolean_variable "USING_EMBEDDED_GRAFANA" "$MSG"
  if [[ $USING_EMBEDDED_GRAFANA ]]; then
    GRAFANA_HOME="$HDH_HOME/grafana-7.0.3.darwin-amd64"
  fi
  MSG="Do you want us to install the historian datasource grafana plugin ? You need it to see data with grafana."
  MSG="$MSG We can install it only if grafana is on this machin as single node otherwise you will have to install it manually."
  MSG="$MSG If you choose to install an embedded grafana you can install it as well."
  ask_and_set_boolean_variable "INSTALLING_DATASOURCE_PLUGIN" "$MSG"
  if [[ $INSTALLING_DATASOURCE_PLUGIN ]]; then
    if [[ -z $GRAFANA_HOME ]]; then
      GRAFANA_PLUGIN_DIR="$GRAFANA_HOME/data/plugins"
    else
      MSG="What is the path to your grafana installation plugin folder ? "
      ask_and_set_variable "GRAFANA_PLUGIN_DIR" "path/to/grafana/data/plugins" "$MSG"
    fi
  fi
  MSG="Do you want us to install an embedded spark (this is not required)?"
  ask_and_set_boolean_variable "INSTALLING_SPARK" "$MSG"
}

print_conf() {
  echo "will start the install with those parameters :"
  echo "HDH_HOME : $HDH_HOME"
  echo "USING_EMBEDDED_SOLR : $USING_EMBEDDED_SOLR"
  echo "SOLR_CLUSTER_URL : $SOLR_CLUSTER_URL"
  echo "CHUNK_COLLECTION_NAME : $CHUNK_COLLECTION_NAME"
  echo "REPORT_COLLECTION_NAME : $REPORT_COLLECTION_NAME"
  echo "USING_EMBEDDED_GRAFANA : $USING_EMBEDDED_GRAFANA"
  echo "INSTALLING_DATASOURCE_PLUGIN : $INSTALLING_DATASOURCE_PLUGIN"
  echo "GRAFANA_PLUGIN_DIR : $GRAFANA_PLUGIN_DIR"
  echo "GRAFANA_HOME : $GRAFANA_HOME"
  echo "TAG_NAMES to add for time series : ${TAG_NAMES[*]}"
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

  echo "$MSG"
  select tags in Add_tags Skip; do
    if [ "$tags" = "Add_tags" ]; then
      echo -e "\n"
      while [ -z "$tag_name" ] || ([ "$tag_name" != 'STOP' ] && [ "$tag_name" != 'stop' ]); do
        read -p "${MSG2} (STOP when you want stop): " tag_name
        if [ "$tag_name" != 'STOP' ] && [ "$tag_name" != '' ] && [ "$tag_name" != 'stop' ]; then
          echo "tapped tag $tag_name"
          _array+=("$tag_name")
        fi
      done
      break
    elif [ "$tags" = "Skip" ]; then
      break
    else
      echo "Please choose 1 or 2"
    fi
  done
  echo "array is ${_array[*]}"
}

#ask user to enter value for the variable, if user just press enter the default value is used instead
#the variable is a boolean (empty string or not)
#param1 Variable name to set
#param2 Msg description
ask_and_set_boolean_variable() {
  local MSG="$2"
  local variable_name_to_modify="$1"
  local -n variable_value="${variable_name_to_modify}"
  echo "${MSG}"
  select solr_install in Standalone Existing; do
    if [ "$solr_install" = "Yes" ]; then
      export "${variable_name_to_modify}=yes"
      break
    elif [ "$solr_install" = "No" ]; then
      export "${variable_name_to_modify}="
      break
    else
      echo "Please choose 1 or 2"
    fi
  done
}

add_tag_names_to_chunk_collection() {
  for tag in "${TAG_NAMES[@]}"; do
    "$HISTORIAN_HOME/bin/modify-collection-schema.sh" -c "$CHUNK_COLLECTION_NAME" -s "$SOLR_CLUSTER_URL" -f "$tag"
    echo -e "\n"
  done
}

extract_historian_into_hdh_home() {
  #HISTORIAN
  mkdir -p "$HDH_HOME" && tar -xf historian-*-bin.tgz -C "$HDH_HOME"
  rm historian-*-bin.tgz
  echo "installed historian into $HDH_HOME"
}

#Install embeded solr if needed and setup SOLR_CLUSTER_URL, default or user input
install_embedded_solr_and_start_it_if_needed() {
  if [[ $USING_EMBEDDED_SOLR ]]; then
    # create 2 data folders for SolR data
    local -r SOLR_NODE_1="$HDH_HOME/data/solr/node1"
    local -r SOLR_NODE_2="$HDH_HOME/data/solr/node2"
    mkdir -p "$SOLR_NODE_1" "$SOLR_NODE_2"
    # touch a few config files for SolR
    local -r SOLR_XML_PATH="$HISTORIAN_HOME/conf/solr.xml"
    echo "${SOLR_XML_PATH}" >"${SOLR_NODE_1}/solr.xml"
    echo "${SOLR_XML_PATH}" >"${SOLR_NODE_2}/solr.xml"
    touch "${HDH_HOME}/data/solr/node1/zoo.cfg"
    # get SolR 8.2.0 and unpack it
    wget https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz
    tar -xf solr-8.2.0.tgz
    rm solr-8.2.0.tgz
    # start a SolR cluster locally with an embedded zookeeper
    local -r SOLR_HOME="$HDH_HOME/solr-8.2.0"
    # démarre un core Solr localement ainsi qu'un serveur zookeeper standalone.
    "${SOLR_HOME}/bin/solr" start -cloud -s "$SOLR_NODE_1" -p 8983
    # démarre un second core Solr localement qui va utiliser le serveur zookeeper précédament créer.
    "${SOLR_HOME}/bin/solr" start -cloud -s "$SOLR_NODE_2" -p 7574 -z localhost:9983
    echo "solr is now running at ${SOLR_CLUSTER_URL}"
    echo -e "\n"
  fi
}

intall_grafana_if_asked() {
  if [[ $USING_EMBEDDED_GRAFANA ]]; then
    wget https://dl.grafana.com/oss/release/grafana-7.0.3.darwin-amd64.tar.gz
    tar -zxvf grafana-7.0.3.darwin-amd64.tar.gz
    rm grafana-7.0.3.darwin-amd64.tar.gz
    brew services restart grafana
  fi
  echo -e "\n"
}

intall_grafana_datasource_plugin_if_asked() {
  if [[ $INSTALLING_DATASOURCE_PLUGIN ]]; then
    wget https://github.com/Hurence/grafana-historian-datasource/archive/v1.0.0.tar.gz
    tar -zxvf grafana-historian-datasource-1.0.0.tar.gz -C "$GRAFANA_PLUGIN_DIR"
    rm grafana-historian-datasource-1.0.0.tar.gz
  fi
}

intall_spark_if_asked() {
  if [[ $INSTALLING_SPARK ]]; then
    # get Apache Spark 2.3.4 and unpack it
    wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz
    tar -xf spark-2.3.4-bin-without-hadoop.tgz
    rm spark-2.3.4-bin-without-hadoop.tgz
    # add two additional jars to spark to handle our framework
    wget -O spark-solr-3.6.6-shaded.jar https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/3.6.6/spark-solr-3.6.6-shaded.jar
    mv spark-solr-3.6.6-shaded.jar "$HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/"
    cp "$HISTORIAN_HOME/lib/loader-*.jar" "$HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/"
  fi
}

generate_historian_server_conf() {
  #Generation du fichier de configuration selon les informations renseignées ( stream_url & chunk_collection )
  echo '{
    "web.verticles.instance.number": 1,
    "historian.verticles.instance.number": 2,
    "http_server" : {
      "host": "localhost",
      "port" : 8080,
      "historian.address": "historian",
      "debug": false,
      "max_data_points_allowed_for_ExportCsv" : 10000,
    },
    "historian": {
      "schema_version": "VERSION_0",
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
  }' >"$HISTORIAN_HOME/conf/historian-server-conf.json"
}

create_historian_collections() {
  # create collection in SolR
  "$HISTORIAN_HOME/bin/create-historian-chunk-collection.sh" -c "$CHUNK_COLLECTION_NAME" -s "$SOLR_CLUSTER_URL"
  echo -e "\n"
  # create report collection in SolR
  "$HISTORIAN_HOME/bin/create-historian-report-collection.sh" -c "$REPORT_COLLECTION_NAME" -s "$SOLR_CLUSTER_URL"
  echo -e "\n"
}

start_historian_server() {
  # and launch the historian REST server
  echo "Install completed. Starting historian..."
  "$HISTORIAN_HOME/bin/historian-server.sh" start
  echo "The historian server is now running."
  echo "You can use ./bin/historian-server.sh [start|stop|restart] to manage the historian server."
}

main() {
  #REQUIEREMENTS
  check_brew_or_install
  check_wget_or_install
  check_maven_or_install
  #Setup conf with user (need to declare arrays in advance)
  declare -a TAG_NAMES=()
  setup_all_variables
  print_conf
  #setup variable from others
  HISTORIAN_HOME="$HDH_HOME/$HISTORIAN_DIR_NAME"
  SOLR_CLUSTER_HISTORIAN_CHUNK_URL="$SOLR_CLUSTER_URL/$CHUNK_COLLECTION_NAME"
  #    Start the installation
  mkdir -p "$HDH_HOME"
  extract_historian_into_hdh_home
  cd "$HDH_HOME" || (echo "could not go to $HDH_HOME folder" && exit 1)
  install_embedded_solr_and_start_it_if_needed
  create_historian_collections
  add_tag_names_to_chunk_collection
  intall_grafana_if_asked
  intall_grafana_datasource_plugin_if_asked
  intall_spark_if_asked
  generate_historian_server_conf
  start_historian_server
  exit 0
}

declare -r DEFAULT_SOLR_CLUSTER_URL="http://localhost:8983/solr"
declare -r HISTORIAN_DIR_NAME="historian-1.3.5"

main "$@"
