#!/bin/bash

set -ue

export RDM_MOUNT_PATH=${MOUNT_PATH:-/mnt}
mkdir -p ${RDM_MOUNT_PATH}

export DEBUG=--debug
export OSF_TOKEN=${RDM_TOKEN}
python3 -m rdmfs.__main__ --allow-other -p ${RDM_NODE_ID} --base-url ${RDM_API_URL} ${DEBUG} ${RDM_MOUNT_PATH} $@
