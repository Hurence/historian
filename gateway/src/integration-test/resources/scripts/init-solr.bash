#!/usr/bin/env bash

pwd
ls

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "DIR is ${SCRIPT_DIR}"

bash "${SCRIPT_DIR}/create-collections.bash"
bash "${SCRIPT_DIR}/inject-annotations-sample.bash"
bash "${SCRIPT_DIR}/inject-chunks-sample.bash"