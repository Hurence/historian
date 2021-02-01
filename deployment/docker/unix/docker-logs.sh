#!/usr/bin/env bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker_common() {
    #shellcheck source=./docker-common.sh
    . "${SCRIPT_DIR}"/docker-common.sh
}

docker_logs() {
    docker-compose -f "${SCRIPT_DIR}"/../compose/docker-compose.yml logs -f
}

docker_common

echo "Opening Hurence Historian logs..."
docker_logs  || {
    echo "Opening Hurence Historian logs... ERROR!"
     exit 1
}

echo "Opening Hurence Historian logs... DONE!"