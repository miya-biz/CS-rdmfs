# rdmfs

## How to run

```
$ docker build -t yacchin1205/rdmfs .
$ docker run -it -v $(pwd)/mnt:/mnt -e RDM_NODE_ID=xxxxx -e RDM_TOKEN=YOUR_PERSONAL_TOKEN -e RDM_API_URL=http://192.168.168.167:8000/v2/ -e MOUNT_PATH=/mnt/test --name rdmfs --privileged yacchin1205/rdmfs
```
