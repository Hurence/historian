#!/bin/bash

################################################################################
# Set needed directories and default variables
################################################################################

CURRENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
HISTORIAN_HOME="$( cd "${CURRENT_SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd )"
HISTORIAN_LIB_DIR="${HISTORIAN_HOME}/lib"
HISTORIAN_CONF_DIR="${HISTORIAN_HOME}/conf"

COMMAND=""
DEBUG="false"
CUSTOM_LOG4J_FILE="false"
USE_VARS_FILE="true"
USE_KERBEROS="false"
HISTORIAN_VERSION="1.3.6-SNAPSHOT"

# Default configuration file path
HISTORIAN_CONFIG_FILE="${HISTORIAN_CONF_DIR}/historian-compactor.yaml"
# Default environment variables file
HISTORIAN_VARS_FILE="${HISTORIAN_CONF_DIR}/historian-compactor-envs"
# Default log4j configuration files
HISTORIAN_LOG4J_FILE="${HISTORIAN_CONF_DIR}/log4j-compactor.properties"
HISTORIAN_LOG4J_DEBUG_FILE="${HISTORIAN_CONF_DIR}/log4j-compactor-debug.properties"

################################################################################
# Functions
################################################################################

# Print usage
usage() {
    SCRIPT_NAME=$(basename ${0})
    cat << EOF

--------------------------------------------------------------------------------
Command to manage the Historian Compactor spark job. Usage:

${SCRIPT_NAME} <command> [options]

<command>:
            help: Print this help then exits.
            start: Start the historian compactor job.
[options]:
            -c|--config-file <config-file-path> : Use configuration file
              different from the default one (${HISTORIAN_CONFIG_FILE}).
            -d|--debug : Enable debug mode. This forces using the log4j debug file
              located at ${HISTORIAN_LOG4J_DEBUG_FILE}.
              Thus, this cannot be used with the -l option.
            -h|--hadoop-config <hadoop-config-path> : The path to the directory
              where the core-site.xml file path resides. If not set, will use
              the HADOOP_CONF_DIR environment variable. If the yarn-site.xml
              file is in the same location as the core-site.xml file then it
              will be used. Otherwise you have to set the YARN_CONF_DIR
              environment variable so that spark-submit knows how to contact the
              YARN resource manager server. This creates or overwrites the
              HADOOP_CONF_DIR environment variable.
            -krb|--kerberos : Enable kerberos authentication. If used,
              principal and keytab options must be set or the corresponding
              environment variables.
            -kt|--keytab-file <keytab-file-path>: Use kerberos with the passed
              keytab file path. Must be used in conjunction with the -krb option.
              This creates or overwrites the KERBEROS_KEYTAB environment variable.
            -l|--log4j-file <log4j-config-file-path> : Use log4j configuration file
              different from the default one (${HISTORIAN_LOG4J_FILE}).
              Cannot be used with the -d option.
            -n|--no-var-file : Do not use any environment variables file (which
              defaults to ${HISTORIAN_VARS_FILE}).
            -p|--principal <principal>: Use kerberos with the passed principal.
              Must be used in conjunction with the -krb option. This creates or
              overwrites the KERBEROS_PRINCIPAL environment variable.
            -s|--spark-home <spark-home-path> : The path to spark home for
              finding the spark-submit command. If not set, will use the
              SPARK_HOME environment variable. This creates or overwrites the
              SPARK_HOME environment variable.
            -v|--var-file <var-file-path> : Use the provided environment
              variables file instead of the default one (${HISTORIAN_VARS_FILE}).
            -y|--yarn-config <yarn-config-path> : The path to the directory
              where the yarn-site.xml file path resides. May be useless if
              yarn-site.xml file is already in <hadoop-config-path> or the
              YARN_CONF_DIR environment variable is set. This creates or
              overwrites the YARN_CONF_DIR environment variable.

Examples:

${SCRIPT_NAME} start
${SCRIPT_NAME} start -c /foobar/custom-historian-compactor.yaml
--------------------------------------------------------------------------------

EOF
}

# Print usage then exits with error
# shellcheck disable=SC2120
print_usage_and_exit_on_error() {
  usage
  EXIT_CODE=1
  if [ $# -ne 0 ]
  then
    # Exit code passed, use it
    EXIT_CODE=${1}
  fi
  exit "${EXIT_CODE}"
}

# Validate option parameter
# Option parameter must present, should not start with '-' or be a
# command.
# Expecting to have the original parameters starting from the option to check
# passed as parameters to this function
# i.e: --config foo bar etc...
# In that example we will check foo (the --config option parameter) exists and
# is valid.
validate_option_parameter() {
  #echo "$@"
  OPTION="${1}"
  shift
  OPTION_PARAM="${1}"

  # Something begin the option?
  if [[ $# -lt 1 ]]
  then
    # shellcheck disable=SC2119
    echo "Missing option ${OPTION} parameter"
    print_usage_and_exit_on_error
  fi

  # Option parameter starts with '-' or is a command ?
  case ${OPTION_PARAM} in
    help|start)
      # Is a command -> error
      echo "Missing option ${OPTION} parameter"
      print_usage_and_exit_on_error
    ;;
    -*)
      # Sounds like an option -> error
      echo "Missing option ${OPTION} parameter"
      print_usage_and_exit_on_error
    ;;
    *)
      # Ok
    ;;
  esac
}

# Parse options given by user
parse_cli_params() {

  while [[ $# -gt 0 ]]
    do
      param="$1"

      case ${param} in
        # Commands
        start)
          COMMAND="start"
          ;;
        help)
            cmd_help
          ;;
        # Options
        -c|--config-file)
          validate_option_parameter "$@"
          HISTORIAN_CONFIG_FILE="${2}"
          shift # Next argument
          ;;
        -d|--debug)
          DEBUG="true"
          HISTORIAN_LOG4J_FILE="${HISTORIAN_LOG4J_DEBUG_FILE}"
          ;;
        -h|--hadoop-config)
          validate_option_parameter "$@"
          TMP_HADOOP_CONF_DIR="${2}"
          shift # Next argument
          ;;
        -krb|--kerberos)
          TMP_USE_KERBEROS="true"
          ;;
        -kt|--keytab-file)
          validate_option_parameter "$@"
          TMP_KERBEROS_KEYTAB="${2}"
          shift # Next argument
          ;;
        -l|--log4j-file)
          validate_option_parameter "$@"
          HISTORIAN_LOG4J_FILE="${2}"
          CUSTOM_LOG4J_FILE="true"
          shift # Next argument
          ;;
        -n|--no-var-file)
          USE_VARS_FILE="false"
          ;;
        -p|--principal)
          validate_option_parameter "$@"
          TMP_KERBEROS_PRINCIPAL="${2}"
          shift # Next argument
          ;;
        -s|--spark-home)
          validate_option_parameter "$@"
          TMP_SPARK_HOME="${2}"
          shift # Next argument
          ;;
        -v|--var-file)
          validate_option_parameter "$@"
          HISTORIAN_VARS_FILE="${2}"
          shift # Next argument
          ;;
        -y|--yarn-config)
          validate_option_parameter "$@"
          TMP_YARN_CONF_DIR="${2}"
          shift # Next argument
          ;;
        # Error if anything else
        *)
          # Unknown parameter
          echo "Unknown command or option: ${param}"
          print_usage_and_exit_on_error
          ;;
      esac
      shift # Next argument
    done
}

check_cli_consistency() {

  # Check a command has been set
  if [[ -z ${COMMAND} ]] # If variable is not set
  then
      echo "Missing command"
      print_usage_and_exit_on_error
  fi

  # Check mutual exclusion for usage of -d and -l options
  if [[ -n ${DEBUG} && "${DEBUG}" == "true" && -n ${CUSTOM_LOG4J_FILE} && "${CUSTOM_LOG4J_FILE}" == "true" ]]
  then
    echo "Cannot use both -d and -l options"
    print_usage_and_exit_on_error
  fi
}

# Overwrite any environment variable that has been overwritten through CLI option
overwrite_variables() {

  if [[ -n ${TMP_SPARK_HOME} ]] # If variable is set
  then
    SPARK_HOME="${TMP_SPARK_HOME}"
  fi

  if [[ -n ${TMP_HADOOP_CONF_DIR} ]] # If variable is set
  then
    HADOOP_CONF_DIR="${TMP_HADOOP_CONF_DIR}"
  fi

  if [[ -n ${TMP_YARN_CONF_DIR} ]] # If variable is set
  then
    YARN_CONF_DIR="${TMP_YARN_CONF_DIR}"
  fi

  if [[ -n ${TMP_USE_KERBEROS} ]] # If variable is set
  then
    USE_KERBEROS="${TMP_USE_KERBEROS}"
  fi

  if [[ -n ${TMP_KERBEROS_PRINCIPAL} ]] # If variable is set
  then
    KERBEROS_PRINCIPAL="${TMP_KERBEROS_PRINCIPAL}"
  fi

  if [[ -n ${TMP_KERBEROS_KEYTAB} ]] # If variable is set
  then
    KERBEROS_KEYTAB="${TMP_KERBEROS_KEYTAB}"
  fi
}

# Read environment variables file if enabled
read_variables_file() {
  # If variable files enabled, read it
  if [[ -n ${USE_VARS_FILE} && "${USE_VARS_FILE}" == "true" ]]
  then
    if [[ ! -a ${HISTORIAN_VARS_FILE} ]]
    then
      # Environment variables file does not exist
      echo "Environment variables file does not exist: ${HISTORIAN_VARS_FILE}"
      print_usage_and_exit_on_error
    fi
    echo "Reading environment variables from ${HISTORIAN_VARS_FILE}"
    source "${HISTORIAN_VARS_FILE}"
  else
    echo "Will not use environment variables file. Just already defined environment variables."
  fi
}

# Resume what will be done and used
display_summary() {
  echo "Command: ${COMMAND}"
  echo "Configuration file: ${HISTORIAN_CONFIG_FILE}"
  echo "Spark Home: ${SPARK_HOME}"
  echo "Hadoop configuration directory: ${HADOOP_CONF_DIR}"
  echo "Yarn configuration directory: ${YARN_CONF_DIR}"
  echo "Debug mode: ${DEBUG}"
  echo "Log4j configuration file: ${HISTORIAN_LOG4J_FILE}"
  echo "Use Kerberos: ${USE_KERBEROS}"
  if [[ -n ${USE_KERBEROS} && "${USE_KERBEROS}" == "true" ]]
  then
    printf "\tKerberos principal: %s\n" "${KERBEROS_PRINCIPAL}"
    printf "\tKerberos keytab: %s\n" "${KERBEROS_KEYTAB}"
  fi
}

# Check variables
check_variables() {

  # Spark
  if [[ -z ${SPARK_HOME} ]] # If variable is not set
  then
    echo "Spark home not specified. Set SPARK_HOME environment variable or use -s|--spark-home option"
    print_usage_and_exit_on_error
  fi

  # Hadoop
  if [[ -z ${HADOOP_CONF_DIR} ]] # If variable is not set
  then
    echo "Hadoop configuration directory not specified. Set HADOOP_CONF_DIR environment variable or use -h|--hadoop-config option"
    print_usage_and_exit_on_error
  fi

  # Kerberos
  if [[ -n ${USE_KERBEROS} && "${USE_KERBEROS}" == "true" ]]
  then
    if [[ -z ${KERBEROS_PRINCIPAL} ]] # If variable is not set
    then
      echo "Need kerberos principal when kerberos is enabled. Set KERBEROS_PRINCIPAL environment variable or use -p|--principal option"
      print_usage_and_exit_on_error
    fi
    if [[ -z ${KERBEROS_KEYTAB} ]] # If variable is not set
    then
      echo "Need kerberos keytab when kerberos is enabled. Set KERBEROS_KEYTAB environment variable or use -kt|--keytab option"
      print_usage_and_exit_on_error
    fi
  fi
}
# Execute the help command
cmd_help() {
  usage
  exit 0
}

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

# If kerberos is requested, prepare spark-submit options for kerberos
prepare_kerberos_options() {

  SPARK_SUBMIT_KERBEROS_OPTIONS=""

  if [[ -n ${USE_KERBEROS} && "${USE_KERBEROS}" == "true" ]]
  then
    SPARK_SUBMIT_KERBEROS_OPTIONS="--principal ${KERBEROS_PRINCIPAL} --keytab ${KERBEROS_KEYTAB}"
  fi
}

# Read spark properties from configuration file
# and prepare spark submit options accordingly
read_spark_properties_from_config_file() {

  READ_APPLICATION_NAME=$(read_property_from_config_file "spark.app.name")
  if [[ -n ${READ_APPLICATION_NAME} ]]
  then
    APPLICATION_NAME="${READ_APPLICATION_NAME}"
  fi

  SPARK_SUBMIT_OPTIONS=""

  # Number of executors
  SPARK_EXECUTOR_INSTANCES=$(read_property_from_config_file "spark.executor.instances")
  if [[ -n ${SPARK_EXECUTOR_INSTANCES} ]]
  then
    SPARK_SUBMIT_OPTIONS="${SPARK_SUBMIT_OPTIONS} --num-executors ${SPARK_EXECUTOR_INSTANCES}"
  fi

  # Driver cores
  SPARK_DRIVER_CORES=$(read_property_from_config_file "spark.driver.cores")
  if [[ -n ${SPARK_DRIVER_CORES} ]]
  then
    SPARK_SUBMIT_OPTIONS="${SPARK_SUBMIT_OPTIONS} --driver-cores ${SPARK_DRIVER_CORES}"
  fi

  # Driver memory
  SPARK_DRIVER_MEMORY=$(read_property_from_config_file "spark.driver.memory")
  if [[ -n ${SPARK_DRIVER_MEMORY} ]]
  then
    SPARK_SUBMIT_OPTIONS="${SPARK_SUBMIT_OPTIONS} --driver-memory ${SPARK_DRIVER_MEMORY}"
  fi

  # Executor cores
  SPARK_EXECUTOR_CORES=$(read_property_from_config_file "spark.executor.cores")
  if [[ -n ${SPARK_EXECUTOR_CORES} ]]
  then
    SPARK_SUBMIT_OPTIONS="${SPARK_SUBMIT_OPTIONS} --executor-cores ${SPARK_EXECUTOR_CORES}"
  fi

  # Executor memory
  SPARK_EXECUTOR_MEMORY=$(read_property_from_config_file "spark.executor.memory")
  if [[ -n ${SPARK_EXECUTOR_MEMORY} ]]
  then
    SPARK_SUBMIT_OPTIONS="${SPARK_SUBMIT_OPTIONS} --executor-memory ${SPARK_EXECUTOR_MEMORY}"
  fi
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
  LOG4J_EXECUTORS_SETTINGS="-Dlog4j.configuration=file:log4j.properties"

  # Run spark-submit command
  CMD="${SPARK_HOME}/bin/spark-submit --master yarn --deploy-mode client \
   ${SPARK_SUBMIT_OPTIONS} \
   ${SPARK_SUBMIT_KERBEROS_OPTIONS} \
   --driver-java-options ${LOG4J_DRIVER_SETTINGS} \
   --conf ${LOG4J_EXECUTORS_SETTINGS} \
   --jars ${COMPACTOR_DEP_JARS} \
   --class ${COMPACTOR_CLASS} \
   --files ${YARN_FILES_OPTIONS} \
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
   --conf ${LOG4J_EXECUTORS_SETTINGS} \
   --jars ${COMPACTOR_DEP_JARS} \
   --class ${COMPACTOR_CLASS} \
   --files ${YARN_FILES_OPTIONS} \
   ${APPLICATION_NAME_OPTION} \
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

# Read passed property ($1) content from the config file.
# Returns the read value. Empty can mean that the property does not exist or is
# set with empty value
read_property_from_config_file() {
  if [[ -z ${1} || ${#} != 1 ]]
  then
    echo "Expecting one parameter at read_property_from_config_file function"
    exit 1
  fi
  # Find the line with the parameter and cut using the ':' yaml separator
  # We ignore any line with # comment character (even if at end of line...)
  PROPERTY_VALUE=$(grep "${1}" "${HISTORIAN_CONFIG_FILE}"|grep -v "#"|cut -d':' -f2)
  # xargs allows to trim any heading/leading space/tab and also remove potential
  # double quotes (key: "value" -> value)
  echo "${PROPERTY_VALUE}" | xargs
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
