#!/bin/bash

################################################################################
# Set needed directories and default variables
################################################################################

CURRENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
HISTORIAN_HOME="$( cd "${CURRENT_SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd )"
HISTORIAN_LIB_DIR="${HISTORIAN_HOME}/lib"
HISTORIAN_CONF_DIR="${HISTORIAN_HOME}/conf"

# Default configuration file path
HISTORIAN_CONFIG_FILE=${HISTORIAN_CONF_DIR}/historian-compactor.yaml
# Default environment variables file
HISTORIAN_VARS_FILE=${HISTORIAN_CONF_DIR}/historian-compactor-envs

################################################################################
# Functions
################################################################################

# Prints usage
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

# Prints usage then exits with error
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
# Parse options given by user
parse_input() {
  #echo "Processing options : $*"

  # Check there is at least one parameter
  if [[ $# -eq 0 ]]
  then
    # shellcheck disable=SC2119
    echo "Missing command"
    print_usage_and_exit_on_error
  fi

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
          if [ "X${2}" == "X" ]
          then
            echo "Missing configuration file after ${param} option"
            print_usage_and_exit_on_error
          fi
          HISTORIAN_CONFIG_FILE="${2}"
          shift # Next argument
        ;;
        # Rest
        *)
          # Unknown parameter
          echo "Unknown command or option: ${param}"
          print_usage_and_exit_on_error
        ;;
      esac
      shift # Next argument
    done
}

################################################################################
# Main
################################################################################

echo ${HISTORIAN_HOME}
echo ${HISTORIAN_LIB_DIR}
echo ${HISTORIAN_CONF_DIR}

# Parse options
echo
parse_input "$@"

echo "Command: ${COMMAND}"
echo "Configuration file: ${HISTORIAN_CONFIG_FILE}"
