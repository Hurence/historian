#!/usr/bin/env bash


print_usage(){
    cat << EOF
    install.sh

    by Hurence, 09/01/2019

EOF
}

check_brew_or_install(){
  #BREW INSTALL
  if [ "$(brew 2>&1)" = "install.sh: line 21: brew: command not found" ]; then
      /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
      brew update
  else
      echo "Brew is already installed"
  fi
}

check_wget_or_install(){
  #WGET INSTALL
  if [ "$(wget 2>&1)" = "install.sh: line 30: wget: command not found" ]; then
      brew install wget
      brew update
  else
      echo "Wget is already installed"
  fi
}

check_maven_or_install(){
  #MAVEN INSTALL
  maven_check=$(mvn 2>&1)
  if [ "$maven_check" = "install.sh: line 40: mvn: command not found" ]; then
      brew install maven
      brew update
  else
      echo "Maven is already installed"
  fi
}

#Create the folder where historian will be installed (asking user the path)
#The given path will be exported into HDH_HOME variable
create_workspace(){
  # create the workspace anywhere you want
  local DEFAULT_INSTALL_DIR="/opt/hdh"
  echo "Do you want to change the default [$DEFAULT_INSTALL_DIR] folder path of Hurence Data Historian?"
  select workspace_location in Change_folder_path Keep_default_path; do
          if [ "$workspace_location" = "Change_folder_path" ]; then
                  echo -e "\n"
                  echo "Where do you want to install Hurence Data Historian ? ex:$DEFAULT_INSTALL_DIR"
                  read -r WORKSPACE
                  mkdir -p "${WORKSPACE}"
                  export HDH_HOME="${WORKSPACE}"
                  break
          elif [ "$workspace_location" = "Keep_default_path" ]; then
                  mkdir -p "$DEFAULT_INSTALL_DIR"
                  export HDH_HOME="$DEFAULT_INSTALL_DIR"
                  break
          else
                  echo "Please choose 1 or 2"
          fi
  done
}

extract_historian_into_hdh_home() {
  #HISTORIAN
  mkdir -p "$HDH_HOME" && tar -xf historian-*-bin.tgz -C "$HDH_HOME"
  rm historian-*-bin.tgz
  echo "installed historian into $HDH_HOME"
}

#Install embeded solr if needed and setup SOLR_CLUSTER_URL, default or user input
install_solr_and_init_solr_variables() {
  #SOLR
  #solr: using an existing solr install or let the historian install solr
  echo "Will you use an existing solr install or let the historian install an embedded solr (version 8.2.0 required) ?"
  select solr_install in Standalone Existing; do
          if [ "$solr_install" = "Standalone" ]; then
                  # create 2 data folders for SolR data
                  local SOLR_NODE_1="$HDH_HOME/data/solr/node1"
                  local SOLR_NODE_2="$HDH_HOME/data/solr/node2"
                  mkdir -p "$SOLR_NODE_1" "$SOLR_NODE_2"
                  # touch a few config files for SolR
                  SOLR_XML="$HDH_HOME/historian/conf/solr.xml"
                  echo "${SOLR_XML}" > "${SOLR_NODE_1}/solr.xml"
                  echo "${SOLR_XML}" > "${SOLR_NODE_2}/solr.xml"
                  touch "${HDH_HOME}/data/solr/node1/zoo.cfg"
                  # get SolR 8.2.0 and unpack it
                  wget https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz
                  tar -xf solr-8.2.0.tgz
                  rm solr-8.2.0.tgz
                  # start a SolR cluster locally with an embedded zookeeper
                  local SOLR_HOME="$HDH_HOME/solr-8.2.0"
                  cd "$SOLR_HOME" || (echo "could not go to $SOLR_HOME folder" && exit 1)
                  # démarre un core Solr localement ainsi qu'un serveur zookeeper standalone.
                  bin/solr start -cloud -s "$SOLR_NODE_1" -p 8983
                  # démarre un second core Solr localement qui va utiliser le serveur zookeeper précédament créer.
                  bin/solr start -cloud -s "$SOLR_NODE_2" -p 7574 -z localhost:9983
                  echo -e "\n"
                  SOLR_CLUSTER_URL="$DEFAULT_SOLR_CLUSTER_URL"
                  break
          elif [ "$solr_install" = "Existing" ]; then
                  #demander le path pour communiquer avec ce serveur : http://hostname:port
                  echo -e "\n"
                  ask_for_solr_url
                  break
          else
                  echo "Please choose 1 or 2"
          fi
  done
}

ask_for_solr_url() {
  echo "What is the path to the solr cluster ? We will use the solr REST api to create collection. Example [$DEFAULT_SOLR_CLUSTER_URL]"
  read -r SOLR_CLUSTER_URL
  SOLR_CLUSTER_URL=${SOLR_CLUSTER_URL:-$DEFAULT_SOLR_CLUSTER_URL}
  echo "SOLR_CLUSTER_URL is $SOLR_CLUSTER_URL" # TODO remove
}

ask_for_chunk_collection_name() {
  echo "Which name to use for the solr collection which will be storing timeseries [historian] ?"
  read -r CHUNK_COLLECTION_NAME           #variable stocké dans $CHUNK_COLLECTION_NAME
  CHUNK_COLLECTION_NAME=${CHUNK_COLLECTION_NAME:-historian}
  echo -e "\n"
}

ask_for_report_collection_name() {
  echo "Which name to use for the solr report collection ? [historian-report]"
  read -r REPORT_COLLECTION_NAME           #variable stocké dans $REPORT_COLLECTION_NAME
  REPORT_COLLECTION_NAME=${REPORT_COLLECTION_NAME:-historian-report}
  echo -e "\n"
}


ask_for_tags_names_and_add_them() {
  cd "$HISTORIAN_HOME" || exit 1
  #Tags names
  echo "Do you want to add tags names for your points ?"
  select tags in Add_tags Skip; do
      if [ "$tags" = "Add_tags" ]; then
        echo -e "\n"
        while [ -z $tags_names ] || [ $tags_names != 'STOP' ]
        do
          read -p 'Tag name (STOP when you want stop): ' tags_names
          if [ $tags_names != 'STOP' ]
          then
            echo -e "\n"
            bin/modify-collection-schema.sh -c $CHUNK_COLLECTION_NAME -s $SOLR_CLUSTER_URL -f $tags_names
            echo -e "\n"
          fi
        done
        break
      elif [ "$tags" = "Skip" ]; then
        break
      else
        echo "Please choose 1 or 2"
      fi
  done
}

intall_grafana() {
  #GRAFANA
  echo "Do you want to install an embedded grafana (version 7.0.3 required) ?"
  select grafana_install in Install_embedded_grafana Skip; do
          if [ "$grafana_install" = "Install_embedded_grafana" ]; then
                  cd "$HDH_HOME" || (echo "could not go to $HDH_HOME" && exit 1)
                  wget https://dl.grafana.com/oss/release/grafana-7.0.3.darwin-amd64.tar.gz
                  tar -zxvf grafana-7.0.3.darwin-amd64.tar.gz
                  rm grafana-7.0.3.darwin-amd64.tar.gz
                  GRAFANA_HOME="$HDH_HOME/grafana-7.0.3.darwin-amd64"
                  brew services restart grafana
                  break
          elif [ "$grafana_install" = "Skip" ]; then
                  break
          else
                  echo "Please choose 1 or 2"
          fi
  done
  echo -e "\n"
}

intall_grafana_datasource_plugin() {

  #GRAFANA PLUGINS
  echo "Do you want to install historian data-source plugin for grafana ?"
  select grafana_plugin_install in Install_grafana_plugin Skip; do
          if [ "$grafana_plugin_install" = "Install_grafana_plugin(only if your grafana is standalone and on this machine)" ]; then
                  cd "$HDH_HOME" || (echo "could not go to $HDH_HOME" && exit 1)
                  wget https://github.com/Hurence/grafana-historian-datasource/archive/v1.0.0.tar.gz
                  if [[ -z $GRAFANA_HOME ]]
                  then
                    echo "your grafana is installed in ${GRAFANA_HOME}"
                  else
                    ask_for_grafana_home_dir
                  fi
                  declare DEFAULT_GRAFANA_PLUGIN_DIR="$GRAFANA_HOME/data/plugins"
                  ask_for_grafana_plugin_dir
                  tar -zxvf grafana-historian-datasource-1.0.0.tar.gz -C "$GRAFANA_PLUGIN_DIR"
                  rm grafana-historian-datasource-1.0.0.tar.gz
                  break
          elif [ "$grafana_plugin_install" = "Skip" ]; then
                  break
          else
                  echo "Please choose 1 or 2"
          fi
  done
  echo -e "\n"
}

ask_for_grafana_plugin_dir() {
  echo "What is the path where your grafana plugin are ? Default is [$DEFAULT_GRAFANA_PLUGIN_DIR]"
  read -r GRAFANA_PLUGIN_DIR
  GRAFANA_PLUGIN_DIR=${GRAFANA_PLUGIN_DIR:-$DEFAULT_GRAFANA_PLUGIN_DIR}
}

ask_for_grafana_home_dir() {
  echo "What is the path where your grafana is intalled ?"
  read -r GRAFANA_HOME
}

intall_spark() {
  #SPARK
  echo "Do you want to install apache spark ?"
  select spark_install in Install_spark Skip; do
          if [ "$spark_install" = "Install_spark" ]; then
                  # get Apache Spark 2.3.4 and unpack it
                  cd "$HDH_HOME" || (echo "could not go to $HDH_HOME" && exit 1)
                  wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz
                  tar -xf spark-2.3.4-bin-without-hadoop.tgz
                  rm spark-2.3.4-bin-without-hadoop.tgz
                  # add two additional jars to spark to handle our framework
                  wget -O spark-solr-3.6.6-shaded.jar https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/3.6.6/spark-solr-3.6.6-shaded.jar
                  mv spark-solr-3.6.6-shaded.jar $HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/
                  cp "$HISTORIAN_HOME/lib/loader-*.jar" "$HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/"
                  break
          elif [ "$spark_install" = "Skip" ]; then
                  break
          else
                  echo "Please choose 1 or 2"
          fi
  done
  echo -e "\n"
}

generate_historian_server_conf() {
  #Generation du fichier de configuration selon les informations renseignées ( stream_url & chunk_collection )
  cd "$HISTORIAN_HOME/conf/" || (echo "could not go into $HISTORIAN_HOME/conf/" && exit 1)

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
        "stream_url" : "'$SOLR_CLUSTER_HISTORIAN_CHUNK_URL'",
        "chunk_collection": "'$CHUNK_COLLECTION_NAME'",
        "annotation_collection": "annotation",
        "sleep_milli_between_connection_attempt" : 10000,
        "number_of_connection_attempt" : 3,
        "urls" : null,
        "connection_timeout" : 10000,
        "socket_timeout": 60000
      }
    }
  }'>historian-server-conf.json
}

create_historian_collections() {
  # create collection in SolR
  cd "$HISTORIAN_HOME" || (echo "could not go into $HISTORIAN_HOME" && exit 1)
  bin/create-historian-chunk-collection.sh -c $CHUNK_COLLECTION_NAME -s $SOLR_CLUSTER_URL
  echo -e "\n"
  # create report collection in SolR
  bin/create-historian-report-collection.sh -c $REPORT_COLLECTION_NAME -s $SOLR_CLUSTER_URL
  echo -e "\n"
}

start_historian_server() {
  # and launch the historian REST server
  cd "$HISTORIAN_HOME" || (echo "could not go into $HISTORIAN_HOME" && exit 1)
  bin/historian-server.sh start

  echo "Install complete. The historian server is now running"
  echo "You can use ./bin/historian-server.sh [start|stop|restart] to manage the historian server."
}

main() {
    #REQUIEREMENTS
    check_brew_or_install
    check_wget_or_install
    check_maven_or_install
    create_workspace # created HDH_HOME variable
    extract_historian_into_hdh_home
    #Variables par défaut
    HISTORIAN_HOME="$HDH_HOME/$HISTORIAN_DIR_NAME"
    CHUNK_COLLECTION_NAME="historian"
    REPORT_COLLECTION_NAME="historian-report"
    cd "$HDH_HOME" || (echo "could not go to $HDH_HOME folder" && exit 1)
    install_solr_and_init_solr_variables
    SOLR_CLUSTER_HISTORIAN_CHUNK_URL="$SOLR_CLUSTER_URL/historian"
    ask_for_chunk_collection_name
    ask_for_report_collection_name
    create_historian_collections
    ask_for_tags_names_and_add_them
    declare GRAFANA_HOME
    intall_grafana
    intall_grafana_datasource_plugin
    intall_spark
    generate_historian_server_conf
    start_historian_servergi
    exit 0
}

declare -r DEFAULT_SOLR_CLUSTER_URL="http://localhost:8983/solr"
declare -r HISTORIAN_DIR_NAME="historian-1.3.5"

main "$@"