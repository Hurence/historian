#!/usr/bin/env bash

declare -r SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_DIR"

print_usage() {
  cat <<EOF
    install.sh

    by Hurence, 09/01/2019

EOF
}

#ask user to enter value for the variable, if user just press enter the default value is used instead
#the variable is a boolean (empty string or not)
#param1 Variable name to set
#param2 Msg description
ask_and_set_boolean_variable() {
  local variable_name_to_modify="$1"
  local MSG="$2"
  echo "$MSG"
  select solr_install in Yes No; do
    case $solr_install in
      Yes)
        export "${variable_name_to_modify}=true"
        break
        ;;
      No)
        export "${variable_name_to_modify}=false"
        break
        ;;
      *)
        echo "Please choose 1 or 2"
        ;;
    esac
  done
}

stop_solr_if_embedded() {
  if [ -d "$HDH_HOME/solr-8.2.0" ]; then
    echo "Stopping embedded solr"
    "$HDH_HOME/solr-8.2.0/bin/solr" stop -all
  fi
}

main() {
  local -r HDH_HOME="$SCRIPT_DIR/../.."
  cd $HDH_HOME
  local MSG="Do you want us to uninstall hurence historian ? It will delete all files where this script is located"
  local MSG="It will delete all files in path : $(pwd)"
  ask_and_set_boolean_variable "UNINSTALL" "$MSG"
  stop_solr_if_embedded
  if [[ $UNINSTALL = true ]]; then
    rm -r *
  fi
  exit 0
}

main "$@"
