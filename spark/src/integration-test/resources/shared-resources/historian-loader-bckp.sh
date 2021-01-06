ROOT_DIR=`dirname $0`
. ${ROOT_DIR}/.properties

echo "Loading data from HDFS / Local FS to SolR"
export HADOOP_CONF_DIR=/usr/hdp/current/hadoop-client/conf/
#export SPARK_MASTER=yarn_cluster
export SPARK_MASTER=local[4]
export HDP_VERSION=3.1.4.0-315
export SPARK_HOME=/usr/hdp/current/spark2-client
#export SPARK_HOME=/home/hurence/adm_hurence_bailett/spark-2.3.4-bin-hadoop2.7
export SPARK_LOCAL_DIR=/disk1/spark_tmp

export SOLR_ZK_BROKERS=islin-hdplnod10.ifp.fr:2181,islin-hdplnod11.ifp.fr:2181,islin-hdplnod12.ifp.fr:2181/solr
export SOLR_CONNECTION_STRING=http://islin-hdplnod10.ifp.fr:8983/solr
export SOLR_HISTORIAN_COLLECTION=historian-v3
export HISTORIAN_HOME=/opt/historian-1.3.6-SNAPSHOT

YARN_CLUSTER_OPTIONS="--master yarn --deploy-mode client --num-executors 10 --executor-cores 4 --executor-memory 4g --driver-memory 2g --driver-cores 1 --principal hurence --keytab conf/hurence.keytab"
DRIVER_EXTRA_JAVA_OPTIONS="spark.driver.extraJavaOptions=${LOG4J_SETTINGS}"
EXECUTOR_EXTRA_JAVA_OPTIONS="spark.executor.extraJavaOptions=${LOG4J_SETTINGS}"
KB_WORKERS_SETTINGS="-Djava.security.auth.login.config=kafka_client_jaas_longrun2.conf -Dsun.security.krb5.debug=true"



LOADER_VERSION="1.3.6-SNAPSHOT"
LOADED_PATH="/disk3/data/dropzone/historian-loaded"
FINISHED_PATH="/disk3/data/dropzone/historian-finished"

CURRENT_YEAR=`date +"%Y"`
CURRENT_MONTH=`date +"%m"`
CURRENT_DAY=`date +"%d"`
IN_PATH29="file://${LOADED_PATH}/ISNTS29-N-${CURRENT_YEAR}-${CURRENT_MONTH}"
IN_PATH35="file://${LOADED_PATH}/ISNTS35-N-${CURRENT_YEAR}-${CURRENT_MONTH}"

echo "Preload and partition evoa csv files from ${IN_PATH29} to Kafka ${KAFKA_CHUNKS_TOPIC}"


IN_PATH="hdfs:///user/hurence/evoa/data/dropzone/ISNTS29-N-2019-02/dataHistorian-ISNTS29-N-201902*"
HISTORIAN_JAR="historian-spark-${LOADER_VERSION}.jar"



loader_batch() {
  echo -e "${LIGHTGRAY}Starting historian daily job @ $DATE ${NOCOLOR}"

  $SPARK_HOME/bin/spark-submit \
      ${YARN_CLUSTER_OPTIONS} \
      --driver-memory 12G \
      --conf "spark.local.dir=${SPARK_LOCAL_DIR}" \
      --class com.hurence.historian.spark.loader.FileLoader  \
      --jars  lib/spark-solr-3.6.6-shaded.jar,lib/${HISTORIAN_JAR}  \
      lib/${HISTORIAN_JAR}  \
      -csv ${IN_PATH}  \
      -groupBy name \
      -zk ${SOLR_ZK_BROKERS} \
      -col ${SOLR_HISTORIAN_COLLECTION} \
      -name tagname \
      -cd ";"  \
      -tags tagname \
      -quality quality \
      -tf "dd/MM/yyyy HH:mm:ss" \
      -origin compactor \
      -dbf "yyyy-MM-dd.HH" \
      -ms yarn-client
}

loader_streaming() {
  echo -e "${LIGHTGRAY}Starting historian streaming job @ $DATE ${NOCOLOR}"

  $SPARK_HOME/bin/spark-submit \
      --driver-memory 4G \
      --conf "spark.local.dir=${SPARK_LOCAL_DIR}" \
      --class com.hurence.historian.spark.loader.FileLoader  \
      --jars  lib/spark-solr-3.6.6-shaded.jar,lib/${HISTORIAN_JAR}  \
      lib/${HISTORIAN_JAR}  \
      -csv "/disk3/data/dropzone/historian/ISNTS35-N-2020-11"  \
      -groupBy name \
      -zk ${SOLR_ZK_BROKERS} \
      -col ${SOLR_HISTORIAN_COLLECTION} \
      -name tagname \
      -cd ";"  \
      -tags tagname \
      -quality quality \
      -tf "dd/MM/yyyy HH:mm:ss" \
      -origin stream-loader \
      -dbf "yyyy-MM-dd.HH" \
      -ms "local[4]" \
      -stream
}


####################################################################
# Script to be launched as hurence user
# Perfom a backup in 4 steps:
# 
# 1. preload, sort and partition daily metrics to HDFS
# 2. compute chunks
# 3. send them to kafka
# 4. cleanup
####################################################################
main() {

  loader_streaming

   # echo -e "${LIGHTGRAY}historian daily job returned ${resultCode} ${NOCOLOR}"
    exit ${resultCode}
}



################################
# GLOBAL VARIABLES
################################
declare -r DATE=$(date +%Y%m%d-%H%M%S)
declare -a HISTORIAN_SERVERS=(29 35)
declare -r LOADER_VERSION="1.3.0"
declare -r CHUNKS_SIZE=1440
declare -r LOADED_PATH="/disk3/data/dropzone/historian-loaded"
declare -r FINISHED_PATH="/disk3/data/dropzone/historian-finished"
declare -r HDFS_OUT="evoa/historian"
declare DRY_RUN=false
declare PREVIOUS_DAY_YEAR=`date --date="-1 day" +"%Y"`
declare PREVIOUS_DAY_MONTH=`date --date="-1 day" +"%m"`
declare PREVIOUS_DAY=`date --date="-1 day" +"%d"`
declare PREVIOUS_DAY_MONTH_NO0=`date --date="-1 day" +"%-m"`
declare PREVIOUS_DAY_NO0=`date --date="-1 day" +"%-d"`

main "$@"
