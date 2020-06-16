#!/usr/bin/env bash


print_usage(){
    cat << EOF
echo modify-collection-schema.sh [options]

[options]:

             -s|--solr-host
             -c|--collection
            -rf|--replication-factor
             -f|--new-field

by Hurence, 09/01/2019

The script modify collections of historian solr

EOF
}

# Parse method is not used for now but we put one exemple here if we want to add arguments
parse_args() {
    echo "parsing command line args"
    POSITIONAL=()
    while [[ $# -gt 0 ]]
    do
        key="$1"

        case $key in
            -s|--solr-host)
                SOLR_HOST="$2"
                shift # past argument
            ;;
            -c|--solr-collection)
                SOLR_COLLECTION="$2"
                shift # past argument
            ;;
            -f|--new-field)
                NEW_FIELD="$2"
                shift # past argument
            ;;
            -h|--help)
                print_usage
                exit 0
            ;;
            *)  # unknown option
                POSITIONAL+=("$1") # save it in an array for later
                shift # past argument
            ;;
        esac
    done

    set -- "${POSITIONAL[@]}" # restore positional parameters
    echo "SOLR_HOST is set to '${SOLR_HOST}'";
    echo "SOLR_COLLECTION is set to '${SOLR_COLLECTION}'"
    echo "NEW_FIELD is set to '${NEW_FIELD}'"

    echo -e "${GREEN}Adding field ${NEW_FIELD} to collection ${SOLR_COLLECTION} for historian on ${SOLR_HOST} ${NOCOLOR}"
}

add_field_to_schema() {
    echo -e "${GREEN}Modifying schema of collection ${SOLR_COLLECTION} ${NOCOLOR}"

    SOLR_UPDATE_QUERY="\"add-field\": { \"name\":\"${NEW_FIELD}\", \"type\":\"string\" }"
    echo "{ ${SOLR_UPDATE_QUERY} }"
    CURL="curl -X POST -H 'Content-type:application/json' \"http://${SOLR_HOST}/${SOLR_COLLECTION}/schema\" --data-binary \"{ ${SOLR_UPDATE_QUERY} }\""
    echo $CURL
    curl -X POST -H 'Content-type:application/json' "http://${SOLR_HOST}/${SOLR_COLLECTION}/schema" --data-binary "{ ${SOLR_UPDATE_QUERY} }"
}


####################################################################
main() {
    parse_args "$@"
    add_field_to_schema
}

################################
# GLOBAL VARIABLES
################################
declare SOLR_HOST="localhost:8983/solr"
declare SOLR_COLLECTION="historian"
declare NEW_FIELD=""
declare MODEL_VERSION=0
declare SOLR_UPDATE_QUERY=""

# color setup
NOCOLOR='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
LIGHTGRAY='\033[0;37m'
DARKGRAY='\033[1;30m'
LIGHTRED='\033[1;31m'
LIGHTGREEN='\033[1;32m'
YELLOW='\033[1;33m'
LIGHTBLUE='\033[1;34m'
LIGHTPURPLE='\033[1;35m'
LIGHTCYAN='\033[1;36m'
WHITE='\033[1;37m'

main "$@"
