# pyetl
A application to read JSON off a streams/kafka queue and make best efforts into ETLing it into a directory in Parquet or JSON formats, or push to a MapR DB. 

## Features
- All
  - Read JSON from Kafka Queue (or MapR streams which is the default)
  - Group aware so you can scale up multiple instance to handle load
  - When using Apache Kafka, can take Zookeeper chroot OR bootstrap brokers
- Mapr-DB
  - Can specify fields, in order, with delimeters, and also add random values for Row Key
  - Mapr Fields in JSON to column families 
- Parquet
  - Use Pandas to infer types of columns
  - Use of Parquet Compression
- JSON
  - GZ compression
- All file outputs
  - Pick a field in your JSON to partition by (Uses Apache Drill Format)
  - Write live to directories or to temp directories

## How to use
- Create the docker file to build using the build_docker.sh script.
  - Right now it allows for arguments, you can make smaller containers by leaving out something (parquet, mapr streams input, and mapr db output can all be left out of the docker build)
  - Running ./build_docker.sh -a will create the docker file in the current directory
- Build the docker container with docker build . (tag it with -t)
- Update run_docker.sh with the name of the image you built and check the other variables
- In the ./bin directory are 4 files. Examples of Streams to maprdb, parq, and json files. (Parquet and Json go to posix mount points for now) and then a master conf file as examples. Create a conf file that matches your setup. 
- Run ./run_docker.sh and then the .sh script in ./bin setup for you. 
- Lots of other ways to run this to, you could just set ENV variables and then run /app/code/pyetl.py directly (for use with Marathon other tools)

## Uses:
- confluent python built against Mapr Librd Kafka
- fastparquet
- pychbase

## Todo:
- Performance optimization testing
- Optimize Docker file. (It's sorta big)
- Currently we only create files to a POSIX compliant Filesystem. Should we look at adding libraries to write direct to HDFS/S3/MapRFS? etc 

