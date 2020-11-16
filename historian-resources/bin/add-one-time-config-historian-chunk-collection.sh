#!/usr/bin/env bash

declare -r SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_DIR"

SCRIPT_NAME=$(basename "$0")

source historian.properties
source solr-util.sh

print_usage(){
    cat << EOF

bash create-historian-collection.sh [options]

[options]:

             -s|--solr-host
             -c|--collection

by Hurence, 19/10/2020

The script modify the config of the historian chunk collection. So it is needed to launch it only one time.

EOF
}

# Parse method is not used for now but we put one exemple here if we want to add arguments
parse_args() {
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
}

add_mlt_request_handler() {
  response_to_add_mlt_request_handler=$(curl -X POST -H 'Content-type:application/json'  --data-binary '
    "add-requesthandler": {
        "name": "/mlt",
        "class": "solr.MoreLikeThisHandler",
        "defaults":{
          "mlt.match.include":true,
          "mlt.fl":"chunk_sax",
          "mlt.minwl":"10",
          "mlt.mindf":"2",
          "mlt.mintf":"1"},
    }' "http://${SOLR_HOST}/${SOLR_COLLECTION}/config")
  if [[ ! $response_to_add_mlt_request_handler == *${TEST_SOLR_CURL_OK}* ]];then
    echo -e "${RED}It seems that add_mlt_request_handler for ${SOLR_COLLECTION} on ${SOLR_HOST} failed !"
    return 1;
  fi
}

add_clustering_request_handler() {
  response_add_clustering_update_searchcomponent=$(curl -X POST -H 'Content-type:application/json'  --data-binary '
    "add-searchcomponent": {
        "name": "clustering",
        "class": "solr.clustering.ClusteringComponent",
        "engine":{
          "name":"lingo",
          "carrot.algorithm":"org.carrot2.clustering.kmeans.BisectingKMeansClusteringAlgorithm"
        },
    }' "http://${SOLR_HOST}/${SOLR_COLLECTION}/config")
  if [[ ! $response_add_clustering_update_searchcomponent == *${TEST_SOLR_CURL_OK}* ]];then
    echo -e "${RED}It seems that creation update-searchcomponent for clustering for ${SOLR_COLLECTION} on ${SOLR_HOST} failed !"
    return 1;
  fi
  response_add_clustering_request_handler=$(curl -X POST -H 'Content-type:application/json'  --data-binary '
  "add-requesthandler": {
      "name": "/clustering",
      "class": "solr.SearchHandler",
      "defaults":{
        "clustering":true,
        "clustering.results":true,
        "carrot.url":id,
        "carrot.title":"name",
        "carrot.snippet":"chunk_sax",
        "rows":100,
        "fl":"*,score"},
      "components": ["clustering"]
  }' "http://${SOLR_HOST}/${SOLR_COLLECTION}/config")
  if [[ ! $response_add_clustering_request_handler == *${TEST_SOLR_CURL_OK}* ]];then
    echo -e "${RED}It seems that add_ing lustering request handler for ${SOLR_COLLECTION} on ${SOLR_HOST} failed !"
    return 1;
  fi
}

####################################################################
main() {
    echo -e "${GREEN}Start of script ${SCRIPT_NAME} ${NOCOLOR}"
    parse_args "$@"
    echo -e "${YELLOW}adding mlt request handler for collection ${SOLR_COLLECTION} on ${SOLR_HOST} ${NOCOLOR}"
    if ! add_mlt_request_handler;then
      echo -e "${RED}add_mlt_request_handler failed${NOCOLOR}"
#              exit 1;#failed
    fi

    echo -e "${YELLOW}adding clustering request handler for collection ${SOLR_COLLECTION} on ${SOLR_HOST} ${NOCOLOR}"
    if ! add_clustering_request_handler;then
      echo -e "${RED}add_clustering_request_handler failed${NOCOLOR}"
#              exit 1;#failed
    fi
    echo -e "${GREEN}End of script ${SCRIPT_NAME} ${NOCOLOR}"
}


main "$@"
