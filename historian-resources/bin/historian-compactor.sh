#!/bin/bash

CURRENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source ${CURRENT_SCRIPT_DIR}/common.sh


################################################################################
# Set needed directories and default variables
################################################################################

# Default configuration file path
HISTORIAN_CONFIG_FILE="${HISTORIAN_CONF_DIR}/historian-compactor.yaml"
# Default log4j configuration files
HISTORIAN_LOG4J_FILE="${HISTORIAN_CONF_DIR}/log4j-compactor.properties"
HISTORIAN_LOG4J_DEBUG_FILE="${HISTORIAN_CONF_DIR}/log4j-compactor-debug.properties"

################################################################################
# Functions
################################################################################


# Execute the start command
cmd_start() {

  echo "Starting Historian Compactor Job..."

  # Export variables needed to spark-submit
  export HADOOP_CONF_DIR
  if [[ -n ${YARN_CONF_DIR} ]] # If variable is set
  then
    export YARN_CONF_DIR
  fi

  # Jar holding the main Compactor class
  COMPACTOR_JAR="${HISTORIAN_LIB_DIR}/historian-compactor-${HISTORIAN_VERSION}.jar"

  # Class to run
  COMPACTOR_CLASS="com.hurence.historian.compactor.Compactor"

  # Create csv list of needed dependency jars
  # Do not put tabs/spaces on lines after the first one or they will appear in
  # the final JARS variable value
  COMPACTOR_DEP_JARS="file:${HISTORIAN_LIB_DIR}/historian-spark-${HISTORIAN_VERSION}.jar,\
file:${HISTORIAN_LIB_DIR}/historian-timeseries-${HISTORIAN_VERSION}.jar"

  # Now get run mode and apply what's asked
  SPARK_MASTER=$(read_property_from_config_file "spark.master")
  case ${SPARK_MASTER} in
    yarn)
      YARN_DEPLOY_MODE=$(read_property_from_config_file "spark.submit.deployMode")
      case ${YARN_DEPLOY_MODE} in
        cluster)
          start_yarn_cluster
          ;;
        client|*) # client mode is the default one if not set in config file
          start_yarn_client
          ;;
      esac
      ;;
    local*)
      # Local mode is for debug only, that why we don't care if it is mandatory
      # to pass hadoop configuration in script options
      start_local
      ;;
    *)
      echo "Unsupported run mode: ${SPARK_MASTER}"
      print_usage_and_exit_on_error
      ;;
  esac
}




# Start compactor job in yarn client mode
start_yarn_client() {
  echo "Starting Compactor Job in YARN client mode"

  # Read any spark property that we support and prepare SPARK_SUBMIT_OPTIONS
  read_spark_properties_from_config_file

  # Prepare spark-submit kerberos options in SPARK_SUBMIT_KERBEROS_OPTIONS
  prepare_kerberos_options

  UPLOADED_LOG4J_CONFIG_FILE="log4j-compactor.properties"
  # Notation: local-file-path1#uploaded-file-name1[,local-file-path2#uploaded-file-name2]
  YARN_FILES_OPTIONS="${HISTORIAN_LOG4J_FILE}#${UPLOADED_LOG4J_CONFIG_FILE}"
  LOG4J_DRIVER_SETTINGS="-Dlog4j.configuration=file:${HISTORIAN_LOG4J_FILE}" # Could use HDFS one like for executors but as driver runs locally...
  LOG4J_EXECUTORS_SETTINGS="-Dlog4j.configuration=file:${UPLOADED_LOG4J_CONFIG_FILE}"

  # Set YARN queue option if requested
  YARN_QUEUE_OPTION=""
  if [[ -n ${YARN_QUEUE} ]]
  then
    YARN_QUEUE_OPTION="--queue ${YARN_QUEUE}"
  fi

  # Run spark-submit command
  CMD="${SPARK_HOME}/bin/spark-submit --master yarn --deploy-mode client \
   ${SPARK_SUBMIT_OPTIONS} \
   ${SPARK_SUBMIT_KERBEROS_OPTIONS} \
   --driver-java-options ${LOG4J_DRIVER_SETTINGS} \
   --conf spark.executor.extraJavaOptions=${LOG4J_EXECUTORS_SETTINGS} \
   --jars ${COMPACTOR_DEP_JARS} \
   --class ${COMPACTOR_CLASS} \
   --files ${YARN_FILES_OPTIONS} \
   ${YARN_QUEUE_OPTION} \
   file:${COMPACTOR_JAR} \
   --config-file ${HISTORIAN_CONFIG_FILE}"
  echo "${CMD}"
  ${CMD}
}

# Start compactor job in yarn cluster mode
start_yarn_cluster() {
  echo "Starting Compactor Job in YARN cluster mode"

  # Read any spark property that we support and prepare SPARK_SUBMIT_OPTIONS
  read_spark_properties_from_config_file

  # Prepare spark-submit kerberos options in SPARK_SUBMIT_KERBEROS_OPTIONS
  prepare_kerberos_options

  UPLOADED_HISTORIAN_CONFIG_FILE="historian-compactor.yaml"
  UPLOADED_LOG4J_CONFIG_FILE="log4j-compactor.properties"
  # Notation: local-file-path1#uploaded-file-name1[,local-file-path2#uploaded-file-name2]
  YARN_FILES_OPTIONS="${HISTORIAN_CONFIG_FILE}#${UPLOADED_HISTORIAN_CONFIG_FILE},${HISTORIAN_LOG4J_FILE}#${UPLOADED_LOG4J_CONFIG_FILE}"
  LOG4J_DRIVER_SETTINGS="-Dlog4j.configuration=file:${UPLOADED_LOG4J_CONFIG_FILE}"
  LOG4J_EXECUTORS_SETTINGS="-Dlog4j.configuration=file:${UPLOADED_LOG4J_CONFIG_FILE}"

  # Set YARN queue option if requested
  YARN_QUEUE_OPTION=""
  if [[ -n ${YARN_QUEUE} ]]
  then
    YARN_QUEUE_OPTION="--queue ${YARN_QUEUE}"
  fi

  # In YARN cluster mode, we have to force application name whereas doing from java code works for other modes (yarn client, local)
  APPLICATION_NAME_OPTION=""
  if [[ -n ${APPLICATION_NAME} ]]
  then
    APPLICATION_NAME_OPTION="--name ${APPLICATION_NAME}"
  fi

  # Run spark-submit command
  CMD="${SPARK_HOME}/bin/spark-submit --master yarn --deploy-mode cluster \
   ${SPARK_SUBMIT_OPTIONS} \
   ${SPARK_SUBMIT_KERBEROS_OPTIONS} \
   --driver-java-options ${LOG4J_DRIVER_SETTINGS} \
   --conf spark.executor.extraJavaOptions=${LOG4J_EXECUTORS_SETTINGS} \
   --jars ${COMPACTOR_DEP_JARS} \
   --class ${COMPACTOR_CLASS} \
   --files ${YARN_FILES_OPTIONS} \
   ${APPLICATION_NAME_OPTION} \
   ${YARN_QUEUE_OPTION} \
   file:${COMPACTOR_JAR} \
   --config-file ${UPLOADED_HISTORIAN_CONFIG_FILE}"
  echo "${CMD}"
  ${CMD}
}

# Start compactor in local mode
start_local() {
  echo "Starting Compactor Job in local mode: ${SPARK_MASTER}"

  # Read any spark property that we support and prepare SPARK_SUBMIT_OPTIONS
  read_spark_properties_from_config_file

  # Prepare spark-submit kerberos options in SPARK_SUBMIT_KERBEROS_OPTIONS
  prepare_kerberos_options

  LOG4J_DRIVER_SETTINGS="-Dlog4j.configuration=file:${HISTORIAN_LOG4J_FILE}"
  LOG4J_EXECUTORS_SETTINGS="-Dlog4j.configuration=file:${HISTORIAN_LOG4J_FILE}"

  # Run spark-submit command
  CMD="${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER} \
   ${SPARK_SUBMIT_OPTIONS} \
   ${SPARK_SUBMIT_KERBEROS_OPTIONS} \
   --driver-java-options ${LOG4J_DRIVER_SETTINGS} \
   --conf ${LOG4J_EXECUTORS_SETTINGS} \
   --jars ${COMPACTOR_DEP_JARS} \
   --class ${COMPACTOR_CLASS} \
   file:${COMPACTOR_JAR} \
   --config-file ${HISTORIAN_CONFIG_FILE}"
  echo "${CMD}"
  ${CMD}
}


################################################################################
# Main
################################################################################

# Parse options
parse_cli_params "$@"

# Check cli command and options consistency
check_cli_consistency

# Read environment variables file if enabled
read_variables_file

# If some options have been set to overwrite some environment variables use them
overwrite_variables

# Check variables
check_variables

echo "Historian home: ${HISTORIAN_HOME}"
echo "Historian conf dir: ${HISTORIAN_CONF_DIR}"
echo "Historian lib dir: ${HISTORIAN_LIB_DIR}"

# Resume what will be done and used
echo
display_summary
echo

case ${COMMAND} in
  start)
    cmd_start
    ;;
  *)
    # Unknown command but not possible as already tested by parsing system!
    echo "Unknown command"
    print_usage_and_exit_on_error
    ;;
esac
