# pyetl
A application to read JSON off a kafka queue and make best efforts into ETLing it into a directory in Parquet format. 

## Features
- Read JSON from Kafka Queue
- Use Pandas to infer types of columns
- Pick a field in your JSON to partition by (Uses Apache Drill Format)
- Use of Parquet Compression
- Run multiple instances to increase through put via Kafka Partitions (and use uniq identifiers!)
- Provide Bootstrap servers OR Zookeepers servers to find brokers

## How to use
- Build the docker container with docker build . (tag it with -t)
- Update run_docker.sh with the name of the image you built
- Update the file in ./bin/run_pyetl.sh to match the environment you are ETLing
- Run ./run_docker.sh
- Lots of other ways to run this to, you could just set ENV variables and then run /app/code/pyparq.py directly (for use with Marathon other tools)

## Uses:
- kafka-python
- fastparquet

## Todo:
- Performance optimization testing
- Use Greenlet or something so we can force a flush of currently queued items on time intervals (it's there, it just has to have a record trigger it)
- Optimize Docker file. (It's sorta big)
- Currently we only create files to a POSIX compliant Filesystem. Should we look at adding libraries to write direct to HDFS/S3/MapRFS? etc 

