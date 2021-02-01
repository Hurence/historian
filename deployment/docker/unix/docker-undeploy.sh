#!/usr/bin/env bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker_common() {
    #shellcheck source=./docker-common.sh
    . "${SCRIPT_DIR}"/docker-common.sh
}

docker_undeploy() {
    docker-compose -f "${SCRIPT_DIR}"/../compose/docker-compose.yml down
}

docker_common

echo "Undeploying Hurence Historian..."
docker_undeploy || { echo "Undeploying Hurence Historian... ERROR!"; exit 1; }
echo "Undeploying Hurence Historian... DONE!"