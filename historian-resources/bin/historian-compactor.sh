#!/bin/bash

################################################################################
# Set needed directories and default variables
################################################################################

CURRENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
HISTORIAN_HOME="$( cd "${CURRENT_SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd )"
HISTORIAN_LIB_DIR="${HISTORIAN_HOME}/lib"
HISTORIAN_CONF_DIR="${HISTORIAN_HOME}/conf"

COMMAND=""
USE_VAR_FILE="true"
USE_KERBEROS="false"

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
Command to manage the historian compactor spark job. Usage:

${SCRIPT_NAME} <command> [options]

<command>:
            help: Print this help then exits.
            start: Start the historian compactor job.
[options]:
            -c|--config <config-file-path> : Use configuration file different
              from the default one (${HISTORIAN_CONFIG_FILE}).
            -n|--no-var-file : Do not use any environment variables file (which
              defaults to ${HISTORIAN_VARS_FILE}).
            -v|--var-file <var-file-path> : Use the provided environment
              variables file instead of the default one (${HISTORIAN_VARS_FILE}).
            -s|--spark-home <spark-home-path> : The path to spark home for
              finding the spark-submit command. If not set, will use the
              SPARK_HOME environment variable. This creates or overwrites the
              SPARK_HOME environment variable.
            -h|--hadoop-config <hadoop-config-path> : The path to the directory
              where the core-site.xml file path resides. If not set, will use
              the HADOOP_CONF_DIR environment variable. If the yarn-site.xml
              file is in the same location as the core-site.xml file then it
              will be used. Otherwise you have to set the YARN_CONF_DIR
              environment variable so that spark-submit knows how to contact the
              YARN resource manager server. This creates or overwrites the
              HADOOP_CONF_DIR environment variable.
            -y|--yarn-config <yarn-config-path> : The path to the directory
              where the yarn-site.xml file path resides. May be useless if
              yarn-site.xml file is already in <hadoop-config-path> or the
              YARN_CONF_DIR environment variable is set. This creates or
              overwrites the YARN_CONF_DIR environment variable.
            -krb|--kerberos : Enable kerberos authentication. If used,
              principal and keytab options must be set or the corresponding
              environment variables.
            -p|--principal <principal>: Use kerberos with the passed principal.
              Must be used in conjunction with the -k option. This creates or
              overwrites the KERBEROS_PRINCIPAL environment variable.
            -kt|--keytab <keytab-file-path>: Use kerberos with the passed keytab
              file path. Must be used in conjunction with the -p option. This
              creates or overwrites the KERBEROS_KEYTAB environment variable.

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
        -n|--no-var-file)
          USE_VAR_FILE="false"
        ;;
        -v|--var-file)
          validate_option_parameter "$@"
          HISTORIAN_VARS_FILE="${2}"
          shift # Next argument
        ;;
        -s|--spark-home)
          validate_option_parameter "$@"
          TMP_SPARK_HOME="${2}"
          shift # Next argument
        ;;
        -h|--hadoop-config)
          validate_option_parameter "$@"
          TMP_HADOOP_CONF_DIR="${2}"
          shift # Next argument
        ;;
        -y|--yarn-config)
          validate_option_parameter "$@"
          TMP_YARN_CONF_DIR="${2}"
          shift # Next argument
        ;;
        -krb|--kerberos)
          TMP_USE_KERBEROS="true"
        ;;
        -p|--principal)
          validate_option_parameter "$@"
          TMP_KERBEROS_PRINCIPAL="${2}"
          shift # Next argument
        ;;
        -kt|--keytab)
          validate_option_parameter "$@"
          TMP_KERBEROS_KEYTAB="${2}"
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
}

# Read environment variables file if enabled
read_variables_file() {
  # If variable files enabled, read it
  if [ ${USE_VAR_FILE} == "true" ]
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
  echo
  echo "Command: ${COMMAND}"
  echo "Configuration file: ${HISTORIAN_CONFIG_FILE}"
  echo "Spark Home: ${SPARK_HOME}"
  echo "Hadoop configuration directory: ${HADOOP_CONF_DIR}"
  echo "Yarn configuration directory: ${YARN_CONF_DIR}"
  echo "Use Kerberos: ${USE_KERBEROS}"
  if [[ -n ${USE_KERBEROS} && "${USE_KERBEROS}" == "true" ]]
  then
    printf "\tKerberos principal: %s\n" "${KERBEROS_PRINCIPAL}"
    printf "\tKerberos keytab: %s\n" "${KERBEROS_KEYTAB}"
  fi
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

echo "Historian home: ${HISTORIAN_HOME}"
echo "Historian conf dir: ${HISTORIAN_CONF_DIR}"
echo "Historian lib dir: ${HISTORIAN_LIB_DIR}"

# Read environment variables file if enabled
read_variables_file

# If some options have been set to overwrite some environment variables use them
overwrite_variables

# Resume what will be done and used
display_summary
