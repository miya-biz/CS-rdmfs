# rdmfs

## How to run

```
$ docker build -t rcosdp/cs-rdmfs .
$ docker run -it -v $(pwd)/mnt:/mnt -e RDM_NODE_ID=xxxxx -e RDM_TOKEN=YOUR_PERSONAL_TOKEN -e RDM_API_URL=http://192.168.168.167:8000/v2/ -e MOUNT_PATH=/mnt/test --name rdmfs --privileged rcosdp/cs-rdmfs
```
