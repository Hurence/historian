#!/usr/bin/env bash


print_usage(){
    cat << EOF
    create-historian-collection.sh [options]

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
            -c|--solr-collection)
                SOLR_COLLECTION="$2"
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
            -d|--dry-run)
                DRY_RUN=true
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

    echo -e "${GREEN}Creating collection for historian on ${SOLR_HOST} ${NOCOLOR}"
}



delete_collection() {
    echo -e "${RED}will delete collection ${SOLR_COLLECTION} on ${SOLR_HOST} ${NOCOLOR}"
    curl "http://${SOLR_HOST}/admin/collections?action=DELETE&name=${SOLR_COLLECTION}"
}

create_collection() {
    echo -e "${GREEN}will create collection ${SOLR_COLLECTION} on ${SOLR_HOST} with ${NUM_SHARDS} shard and ${REPLICATION_FACTOR} replicas ${NOCOLOR}"
    curl "http://${SOLR_HOST}/admin/collections?action=CREATE&name=${SOLR_COLLECTION}&numShards=${NUM_SHARDS}&replicationFactor=${REPLICATION_FACTOR}"


    echo "waiting 5' for changes propagation"
    sleep 5
    
    SOLR_UPDATE_QUERY='
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


add_field() {
  SOLR_UPDATE_QUERY="\"add-field\": { \"name\":\"$1\", \"type\":\"$2\" }, ${SOLR_UPDATE_QUERY}"
}

add_field_multivalued() {
  SOLR_UPDATE_QUERY="\"add-field\": { \"name\":\"$1\", \"type\":\"$2\", \"multiValued\":true} }, ${SOLR_UPDATE_QUERY}"
}

add_field_not_indexed() {
  SOLR_UPDATE_QUERY="\"add-field\": { \"name\":\"$1\", \"type\":\"$2\", \"indexed\":false }, ${SOLR_UPDATE_QUERY}"
}

delete_field() {
    curl -X POST -H 'Content-type:application/json' -d "{ \"delete-field\":{\"name\":\"$1\"}}" "http://${SOLR_HOST}/${SOLR_COLLECTION}/schema"
}

create_schema() {

    echo -e "${GREEN}Creating schema for historian version ${MODEL_VERSION} ${NOCOLOR}"

    case ${MODEL_VERSION} in
        "0")
            add_field "chunk_start" "plong"
            add_field "chunk_end" "plong"
            add_field "chunk_value" "text_general"
            add_field "chunk_avg" "pdouble"
            add_field "chunk_size_bytes" "pint"
            add_field "chunk_size" "pint"
            add_field "chunk_count" "pint"
            add_field "chunk_min" "pdouble"
            add_field "chunk_max" "pdouble"
            add_field "chunk_sax" "ngramtext"
            add_field "chunk_trend" "boolean"
            add_field "chunk_window_ms" "plong"
            add_field "chunk_first" "pdouble"
            add_field "timestamp" "plong"
            add_field "value" "pdouble"
            add_field "tagname" "text_general"
            add_field "name" "text_general"
            add_field "record_value" "pdouble"
            add_field "code_install" "text_general"
            add_field "sensor" "text_general"
            add_field "quality" "pfloat"
            add_field "year" "pint"
            add_field "month" "pint"
            add_field "day" "pint"
            add_field "hour" "pint"
            ;;
        "1")
            add_field_not_indexed "chunk_value" "string"
            add_field "chunk_start" "plong"
            add_field "chunk_end" "plong"
            add_field "chunk_avg" "pdouble"
            add_field "chunk_size_bytes" "pint"
            add_field "chunk_count" "pint"
            add_field "chunk_min" "pdouble"
            add_field "chunk_max" "pdouble"
            add_field "chunk_sax" "ngramtext"
            add_field "chunk_trend" "boolean"
            add_field "chunk_outlier" "boolean"
            add_field "chunk_window_ms" "plong"
            add_field "chunk_first" "pdouble"
            add_field "chunk_sum" "pdouble"
            add_field "chunk_origin" "string"
            add_field "chunk_api_version" "string"
            add_field_multivalued "chunk_attributes" "string"
            add_field_multivalued "chunk_tags" "string"
            add_field_multivalued "chunk_qualities" "string"
            add_field "name" "string"
            add_field "timestamp" "plong"
            add_field "year" "pint"
            add_field "month" "pint"
            add_field "day" "string"
            add_field "hour" "pint"
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
    create_collection
    create_schema
}

################################
# GLOBAL VARIABLES
################################
declare SOLR_HOST="localhost:8983/solr"
declare SOLR_COLLECTION="historian"
declare REPLICATION_FACTOR=1
declare NUM_SHARDS=2
declare DRY_RUN=false
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












