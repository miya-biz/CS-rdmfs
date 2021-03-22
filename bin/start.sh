#!/bin/bash

set -ue

export OSF_TOKEN=${RDM_TOKEN}
python3 -m rdmfs.__main__ -p ${RDM_NODE_ID} --base-url ${RDM_API_URL} --debug ${MNT_PATH:-/mnt}
