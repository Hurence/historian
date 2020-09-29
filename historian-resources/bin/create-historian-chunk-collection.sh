#!/usr/bin/env bash

declare -r SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_DIR"
source solr-util.sh

################################
# GLOBAL VARIABLES
################################
declare SOLR_HOST="localhost:8983/solr"
declare SOLR_COLLECTION="historian"
declare SOLR_FIELD_NAME=""
declare SOLR_FIELD_TYPE="text_general"
declare REPLICATION_FACTOR=1
declare NUM_SHARDS=2
declare DRY_RUN=false
declare MODEL_VERSION="VERSION_0"
declare SOLR_UPDATE_QUERY=""
declare UPDATE_MODE="create-collection"



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

print_usage(){
    cat << EOF

bash create-historian-collection.sh [options]

[options]:

             -s|--solr-host
             -c|--collection
            -fn|--field-name
            -ft|--field-type
            -rf|--replication-factor
            -ns|--num-shards
             -d|--dry-run
             -v|--model-version
             -m|--update-mode

by Hurence, 09/01/2019

The script creates a collection for historian solr

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
            -c|--collection)
                SOLR_COLLECTION="$2"
                shift # past argument
            ;;
            -fn|--field-name)
                SOLR_FIELD_NAME="$2"
                shift # past argument
            ;;
            -ft|--field-type)
                SOLR_FIELD_TYPE="$2"
                shift # past argument
            ;;
            -rf|--replication-factor)
                REPLICATION_FACTOR="$2"
                shift # past argument
            ;;
            -ns|--num-shards)
                NUM_SHARDS="$2"
                shift # past argument
            ;;
            -d|--dry-run)
                DRY_RUN=true
                shift # past argument
            ;;
            -v|--model-version)
                MODEL_VERSION="$2"
                shift # past argument
            ;;
            -m|--update-mode)
                UPDATE_MODE="$2"
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
    echo "REPLICATION_FACTOR is set to '${REPLICATION_FACTOR}'"
    echo "NUM_SHARDS is set to '${NUM_SHARDS}'"
    echo "DRY_RUN is set to '${DRY_RUN}'"
    echo "MODEL_VERSION is set to '${MODEL_VERSION}'"


}

add_ngramtext_type_to_collection() {
  append_string_to_variable SOLR_UPDATE_QUERY '
      "add-field-type" : {
         "name":"ngramtext",
         "class":"solr.TextField",
         "positionIncrementGap":"100",
         "indexAnalyzer" : {
            "tokenizer":{
               "class":"solr.NGramTokenizerFactory",
               "minGramSize":"2",
               "maxGramSize":"10"  },
            "filters":[{
               "class":"solr.LowerCaseFilterFactory" }]
          },
          "queryAnalyzer" : {
           "type": "query",
            "tokenizer":{
               "class":"solr.StandardTokenizerFactory" },
            "filters":[{
               "class":"solr.LowerCaseFilterFactory" }]
          }
        }'
}

create_schema() {

    echo -e "${GREEN}Creating schema of chunk collection for ${SOLR_COLLECTION} version ${MODEL_VERSION} ${NOCOLOR}"

    case ${MODEL_VERSION} in
        "EVOA0")
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_start" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_end" "plong"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"compactions_running\", \"type\":\"string\", \"indexed\":true, \"multiValued\":true, \"stored\" : true }"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"chunk_value\", \"type\":\"string\", \"indexed\":false, \"multiValued\":false }"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_origin" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "numeric_type" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_avg" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_size_bytes" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_size" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_count" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_min" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_max" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_sum" "pdoubles"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_sax" "ngramtext"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_trend" "boolean"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_outlier" "booleans"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_window_ms" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_first" "pdoubles"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "record_errors" "text_general"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"name\", \"type\":\"string\", \"multiValued\":false }"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "record_id" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "record_name" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "record_time" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "record_type" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "record_value" "pdouble"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"sensor\", \"type\":\"string\", \"multiValued\":false }"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"tagname\", \"type\":\"string\", \"multiValued\":false }"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "timestamp" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "value" "pdoubles"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "week" "plongs"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "year" "plongs"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"code_install\", \"type\":\"string\", \"multiValued\":false }"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "quality" "pfloat"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "month" "plongs"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "day" "plongs"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "hour" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "delete" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "file_path" "text_general"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_last" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_stddev" "pdouble"
            ;;
        "VERSION_0")
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"name\", \"type\":\"string\", \"indexed\":true, \"multiValued\":false, \"required\":true, \"stored\" : true }"
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"compactions_running\", \"type\":\"string\", \"indexed\":true, \"multiValued\":true, \"stored\" : true }"
            add_field_not_indexed_to_variable "SOLR_UPDATE_QUERY" "chunk_value" "string"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_start" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_end" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_avg" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_count" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_min" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_max" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_sax" "ngramtext"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_trend" "boolean"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_origin" "string"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_outlier" "boolean"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_first" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_last" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_stddev" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_sum" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_year" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_month" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_day" "string"
            ;;
          "VERSION_1")
            SOLR_UPDATE_QUERY="${SOLR_UPDATE_QUERY}, \"add-field\": { \"name\":\"name\", \"type\":\"string\", \"indexed\":true, \"multiValued\":false, \"required\":true, \"stored\" : true }"
            add_field_not_indexed_to_variable "SOLR_UPDATE_QUERY" "version" "string"
            add_field_not_indexed_to_variable "SOLR_UPDATE_QUERY" "metric_key" "string"
            add_field_not_indexed_to_variable "SOLR_UPDATE_QUERY" "chunk_value" "string"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_start" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_end" "plong"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_avg" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_count" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_min" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_max" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_sax" "ngramtext"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_trend" "boolean"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_origin" "string"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_outlier" "boolean"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_first" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_last" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_std_dev" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_sum" "pdouble"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_year" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_month" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_day" "string"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_hour" "pint"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_quality_avg" "pfloat"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_quality_min" "pfloat"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_quality_max" "pfloat"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_quality_sum" "pfloat"
            add_field_name_type_to_variable "SOLR_UPDATE_QUERY" "chunk_quality_first" "pfloat"
            ;;
        *)
            echo -e "${RED}Unsupported historian version ${MODEL_VERSION}, exiting...${NOCOLOR}"
            exit 0
            ;;
    esac


    echo "{ ${SOLR_UPDATE_QUERY} }"
    curl -X POST -H 'Content-type:application/json' "http://${SOLR_HOST}/${SOLR_COLLECTION}/schema" --data-binary "{ ${SOLR_UPDATE_QUERY} }"

}


####################################################################
main() {
    parse_args "$@"
    case ${UPDATE_MODE} in
        "create-collection")
            echo -e "${GREEN}Creating collection for ${SOLR_COLLECTION} on ${SOLR_HOST} ${NOCOLOR}"
            create_collection "${SOLR_HOST}" "${SOLR_COLLECTION}"
            add_ngramtext_type_to_collection
            create_schema
            ;;
        "add-field")
            echo -e "${GREEN}Add field ${SOLR_FIELD_NAME} of type ${SOLR_FIELD_TYPE} to collection ${SOLR_COLLECTION} on ${SOLR_HOST} ${NOCOLOR}"
            add_field_to_variable "SOLR_UPDATE_QUERY" "${SOLR_FIELD_NAME}" "${SOLR_FIELD_TYPE}"
            curl -X POST -H 'Content-type:application/json' "http://${SOLR_HOST}/${SOLR_COLLECTION}/schema" --data-binary "{ ${SOLR_UPDATE_QUERY} }"
            ;;
        *)
            echo -e "${RED}Unknown update mode option ${UPDATE_MODE}, doing nothing and exiting...${NOCOLOR}"
            exit 0
            ;;
    esac

}


main "$@"
