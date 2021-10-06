#!/bin/bash

set -ue

export RDM_MOUNT_PATH=${MOUNT_PATH:-/mnt}
mkdir -p ${RDM_MOUNT_PATH}

export RDM_MOUNT_FILE_MODE=${MOUNT_FILE_MODE:-0666}
export RDM_MOUNT_DIR_MODE=${MOUNT_DIR_MODE:-0777}

export DEBUG=--debug
export OSF_TOKEN=${RDM_TOKEN}
python3 -m rdmfs.__main__ \
    --file-mode ${RDM_MOUNT_FILE_MODE} \
    --dir-mode ${RDM_MOUNT_DIR_MODE} \
    --allow-other -p ${RDM_NODE_ID} \
    --base-url ${RDM_API_URL} ${DEBUG} ${RDM_MOUNT_PATH} $@
