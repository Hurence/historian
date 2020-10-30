#!/bin/bash

################################################################################
# Set needed directories and default variables
################################################################################

CURRENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
HISTORIAN_HOME="$( cd "${CURRENT_SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd )"
HISTORIAN_LIB_DIR="${HISTORIAN_HOME}/lib"
HISTORIAN_CONF_DIR="${HISTORIAN_HOME}/conf"

COMMAND=""
COMPACTOR_DEPLOY_MODE="cluster"
USE_VARS_FILE="true"
USE_KERBEROS="false"
HISTORIAN_VERSION="1.3.6-SNAPSHOT"

# Default configuration file path
HISTORIAN_CONFIG_FILE=${HISTORIAN_CONF_DIR}/historian-compactor.yaml
# Default environment variables file
HISTORIAN_VARS_FILE=${HISTORIAN_CONF_DIR}/historian-compactor-envs

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
            -c|--config <config-file-path> : Use configuration file different
              from the default one (${HISTORIAN_CONFIG_FILE}).
            -cl|--client-mode : Run job in client mode. Default is use cluster
              mode in which the spark driver runs anywhere on the cluster
              whereas client mode makes the driver run where you run the
              spark-submit command. This creates or overwrites the
              COMPACTOR_DEPLOY_MODE environment variable whose default is cluster.
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
            -kt|--keytab <keytab-file-path>: Use kerberos with the passed keytab
              file path. Must be used in conjunction with the -krb option. This
              creates or overwrites the KERBEROS_KEYTAB environment variable.
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
            usage
            exit 0
        ;;
        # Options
        -c|--config)
          validate_option_parameter "$@"
          HISTORIAN_CONFIG_FILE="${2}"
          shift # Next argument
        ;;
        -cl|--client-mode)
          TMP_COMPACTOR_DEPLOY_MODE="client"
        ;;
        -h|--hadoop-config)
          validate_option_parameter "$@"
          TMP_HADOOP_CONF_DIR="${2}"
          shift # Next argument
        ;;
        -krb|--kerberos)
          TMP_USE_KERBEROS="true"
        ;;
        -kt|--keytab)
          validate_option_parameter "$@"
          TMP_KERBEROS_KEYTAB="${2}"
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

  if [[ -n ${TMP_COMPACTOR_DEPLOY_MODE} ]] # If variable is set
  then
    COMPACTOR_DEPLOY_MODE="${TMP_COMPACTOR_DEPLOY_MODE}"
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
  echo "Yarn deploy mode: ${COMPACTOR_DEPLOY_MODE}"
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

  # Yarn
  echo "deploy mode: ${COMPACTOR_DEPLOY_MODE}"
  if [[ -z ${COMPACTOR_DEPLOY_MODE} || "${COMPACTOR_DEPLOY_MODE}" != "client" && "${COMPACTOR_DEPLOY_MODE}" != "cluster" ]]
  then
    echo "COMPACTOR_DEPLOY_MODE must be set to 'client' or 'cluster'. COMPACTOR_DEPLOY_MODE value is: '${COMPACTOR_DEPLOY_MODE}'"
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

# Execute the start command
cmd_start() {

  echo "Starting Historian Compactor Job..."

  # Export variables needed to spark-submit
  export HADOOP_CONF_DIR
  if [[ -n ${YARN_CONF_DIR} ]] # If variable is set
  then
    export YARN_CONF_DIR
  fi

  # Create csv list of needed dependency jars
  # Do not put tabs/spaces on all lines or they will appear in the final JARS variable value
  JARS="${HISTORIAN_LIB_DIR}/historian-compactor-${HISTORIAN_VERSION}.jar,\
${HISTORIAN_LIB_DIR}/historian-spark-${HISTORIAN_VERSION}.jar,\
${HISTORIAN_LIB_DIR}/historian-timeseries-${HISTORIAN_VERSION}.jar"

  # config file .get, class, deploy mode option, logs, debug mode ?

  #"${SPARK_HOME}"/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-memory 2G --executor-cores 4 --class org.apache.spark.examples.SparkPi ~/addons/spark/examples/jars/spark-examples_2.11-2.3.2.jar
}

################################################################################
# Main
################################################################################

# Parse options
parse_cli_params "$@"

# Check a command has been set
if [[ -z ${COMMAND} ]] # If variable is not set
then
    echo "Missing command"
    print_usage_and_exit_on_error
fi

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
