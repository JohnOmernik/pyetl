#!/bin/bash

IMG="dockerregv2-shared.marathon.slave.mesos:5005/pyetl:1.0.0"

CODE="-v=`pwd`:/app/code:rw"

BIN="-v=`pwd`/bin:/app/bin:rw"
#DATA="-v=/your/path/to/data:/app/data:rw"
MAPR="-v=/opt/mapr:/opt/mapr:ro"


CMD="/app/bin/run_pyetl.sh"
CMD="/bin/bash"

# Alternatively, you can just set the environmental variables set in ./bin/run_pyetl.sh and then call /app/code/pyparq.py directly

sudo docker run -it --rm $BIN $CODE $MAPR $IMG $CMD
