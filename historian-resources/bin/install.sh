#!/usr/bin/env bash


print_usage(){
    cat << EOF
    installreadme.sh [options]

    by Hurence, 09/01/2019

EOF
}

parse_args(){
#Variables par défaut

solr_cluster_path=http://localhost:8983/solr/historian
chunk_collection_name=historian

#REQUIEREMENTS
#BREW INSTALL
brew_ckeck=$(brew 2>&1)
  if [ "$brew_check" = "install.sh: line 21: brew: command not found" ]; then
                /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
                brew update
        else
                echo "Brew is already installed"
        fi
echo -e "\n"
#WGET INSTALL
wget_ckeck=$(wget 2>&1)
  if [ "$wget_check" = "install.sh: line 30: wget: command not found" ]; then
                brew install wget
                brew update
        else
                echo "Wget is already installed"
        fi
echo -e "\n"

#MAVEN INSTALL
maven_check=$(mvn 2>&1)
  if [ "$maven_check" = "install.sh: line 40: mvn: command not found" ]; then
                brew install maven
                brew update
        else
                echo "Maven is aldready installed"
        fi
echo -e "\n"

#WORKSPACE
# create the workspace anywhere you want
echo "Do you want to change the default [~] folder path of Hurence Data Historian?"
select workspace_location in Change_folder_path Keep_default_path; do
        if [ "$workspace_location" = "Change_folder_path" ]; then
                echo -e "\n"
                echo "Where do you want to install Hurence Data Historian ? ex:/Users/Antoine/Documents"
                read WORKSPACE
                mkdir -p "${WORKSPACE}"
                export "HDH_HOME=${WORKSPACE}"
                break
        elif [ "$workspace_location" = "Keep_default_path" ]; then
                mkdir ~
                export HDH_HOME=~
                break
        else
                echo "Please choose 1 or 2"
        fi
done
echo -e "\n"

#HISTORIAN
#Pas besoin du telechargement  wget https://github.com/Hurence/historian/releases/download/v1.3.4/historian-1.3.4-SNAPSHOT.tgz
cp historian-1.3.4-SNAPSHOT.tgz $HDH_HOME
rm historian-1.3.4-SNAPSHOT.tgz
cd $HDH_HOME
tar -xf historian-1.3.4-SNAPSHOT.tgz
rm historian-1.3.4-SNAPSHOT.tgz

#SOLR
#solr: using an existing solr install or let the historian install solr
echo "Will you use an existing solr install or let the historian install an embedded solr (version 8.2.0 required) ?"
select solr_install in Standalone Existing; do
        if [ "$solr_install" = "Standalone" ]; then
                # create 2 data folders for SolR data
                mkdir -p $HDH_HOME/data/solr/node1 $HDH_HOME/data/solr/node2
                # touch a few config files for SolR
                SOLR_XML=$HDH_HOME/historian-1.3.4-SNAPSHOT/conf/solr.xml
                echo ${SOLR_XML} > ${HDH_HOME}/data/solr/node1/solr.xml
                echo ${SOLR_XML} > ${HDH_HOME}/data/solr/node2/solr.xml
                touch ${HDH_HOME}/data/solr/node1/zoo.cfg
                # get SolR 8.2.0 and unpack it
                cd $HDH_HOME
                wget https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz
                tar -xf solr-8.2.0.tgz
                rm solr-8.2.0.tgz
                # start a SolR cluster locally with an embedded zookeeper
                cd $SOLR_HOME
                # démarre un core Solr localement ainsi qu'un serveur zookeeper standalone.
                bin/solr start -cloud -s $SOLR_HOME/data/solr/node1 -p 8983
                # démarre un second core Solr localement qui va utiliser le serveur zookeeper précédament créer.
                bin/solr start -cloud -s $SOLR_HOME/data/solr/node2/ -p 7574 -z localhost:9983
                echo -e "\n"
                echo "Which name to use for the solr collection which will be storing timeseries ?"
                read chunk_collection_name           #variable stocké dans $chunk_collection_name
                break
        elif [ "$solr_install" = "Existing" ]; then
                #demander le path pour communiquer avec ce serveur : http://hostname:port
                echo -e "\n"
                echo "What is the path to the solr cluster ? We will use the solr REST api to create collection "
                read solr_cluster_path               #variable stocké dans $solr_cluster_path
                #demander le nom de la collection qui vas stocker les timeseries
                echo -e "\n"
                echo "Which name to use for the solr collection which will be storing timeseries ?"
                read chunk_collection_name           #variable stocké dans $chunk_collection_name
                break
        else
                echo "Please choose 1 or 2"
        fi
done

echo -e "\n"
echo "Which name to use for the solr report collection ?"
read report_collection_name           #variable stocké dans $chunk_collection_name
echo -e "\n"

#GRAFANA
echo "Do you want to install an embedded grafana (version 7.0.3 required) ?"
select grafana_install in Install_embedded_grafana Skip; do
        if [ "$grafana_install" = "Install_embedded_grafana" ]; then
                cd $HDH_HOME
                brew install grafana
                #install this plugin using the grafana-cli tool:
                sudo grafana-cli plugins install grafana-simple-json-datasource
                brew services restart grafana
                break
        elif [ "$grafana_install" = "Skip" ]; then
                break
        else
                echo "Please choose 1 or 2"
        fi
done
echo -e "\n"

#SPARK
echo "Do you want to install apache spark ?"
select spark_install in Install_spark Skip; do
        if [ "$spark_install" = "Install_spark" ]; then
                # get Apache Spark 2.3.4 and unpack it
                cd $HDH_HOME
                wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz
                tar -xf spark-2.3.4-bin-without-hadoop.tgz
                rm spark-2.3.4-bin-without-hadoop.tgz
                # add two additional jars to spark to handle our framework
                wget -O spark-solr-3.6.6-shaded.jar https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/3.6.6/spark-solr-3.6.6-shaded.jar
                mv spark-solr-3.6.6-shaded.jar $HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/
                cp $HDH_HOME/historian-1.3.4-SNAPSHOT/lib/loader-1.3.4-SNAPSHOT.jar $HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/
                break
        elif [ "$spark_install" = "Skip" ]; then
                break
        else
                echo "Please choose 1 or 2"
        fi
done
echo -e "\n"

#Generation du fichier de configuration selon les informations renseignées ( stream_url & chunk_collection )

cd $HDH_HOME/historian-1.3.4-SNAPSHOT/conf/

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
      "stream_url" : "'$solr_cluster_path'",
      "chunk_collection": "'$chunk_collection_name'",
      "annotation_collection": "annotation",
      "sleep_milli_between_connection_attempt" : 10000,
      "number_of_connection_attempt" : 3,
      "urls" : null,
      "connection_timeout" : 10000,
      "socket_timeout": 60000
    }
  }
}'>historian-server-conf.json

# create collection in SolR
cd $HDH_HOME/historian-1.3.4-SNAPSHOT
bin/create-historian-chunk-collection.sh -c $chunk_collection_name -s $solr_cluster_path
echo -e "\n"
bin/create-historian-chunk-collection.sh -s $solr_cluster_path
echo -e "\n"
# create report collection in SolR
bin/create-historian-report-collection.sh -c $report_collection_name -s $solr_cluster_path
echo -e "\n"
bin/create-historian-report-collection.sh -c historian_report -s $solr_cluster_path
echo -e "\n"

# and launch the historian REST server
bin/historian-server.sh start

echo "Install complete"

exit 0
}

main() {
    parse_args "$@"
    create_collection
    create_schema
}

main "$@"