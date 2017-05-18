#!/bin/bash

# You must provide Bootstrap servers (kafka nodes and their ports OR Zookeepers and the kafka ID of the chroot for your kafka instance
export ZOOKEEPERS="node1:2181,node2:2181,node3:2181"
export KAFKA_ID="kafkainstance"
# OR
# export BOOTSTRAP_BROKERS="node1:9000,node2:9000"

# This is the name of the consumer group your client will create/join. If you are running multiple instances this is great, name them the same and Kafka will partition the info 
export GROUP_ID="mylogs_groups"

# When registering a consumer group, do you want to start at the first data in the queue (earliest) or the last (latest)
export OFFSET_RESET="earliest"

# The Topic to connect to
export TOPIC="mylogs"

# The next three items has to do with the cacheing of records. As this come off the kafka queue, we store them in a list to keep from making smallish writes and dataframes
# These are very small/conservative, you should be able to increase, but we need to do testing at volume

export ROWMAX=500 # Total max records cached. Regardless of size, once the number of records hits this number, the next record will cause a flush and write to parquet
export SIZEMAX=256000  # Total size of records. This is a rough running size of records in bytes. Once the total hits this size, the next record will cause a flush and write to parquet
export TIMEMAX=60  # seconds since last write to force a write # The number of seconds since the last flush. Once this has been met, the next record from KAfka will cause a flush and write. 

# This is the max size of (records) of a row group in a single Parquet write. If a single write exceeds this, another row group will be created in the file
export PARQ_OFFSETS=50000000

# What compression the Parquet file will be written with
export PARQ_COMPRESS="SNAPPY"

# As cached records are flush and appeneded to the current file, the file grows. This is the maximum size in bytes that the file will get. When it's reached, pyetl will create a new file
export FILEMAXSIZE=8000000
# Each individual append creates a row group. So if you have small appends, you could have lots of row groups in a single file which is inefficient. If you set this to 1, then
# pyetl will, when the max file size is reached, read the WHOLE file into a dataframe and write it back out.  If the number of records is below PARQ_OFFSETS then there will only be
# one row group making subsequent reads faster. We've only tested this on smallish files, needs some modeling to test. 
export MERGE_FILE=1

# Since we can have multiple pyetl instances running (partitions/consumer groups etc) We need some sort of uniq value so when writing file names, we don't clobber multiple instance
# files.  This is easy if you are running in Docker, using Bridged networking. We just use the HOSTNAME env variable.  If you are not running in  Docker, or you are running in host network mode
# where you could have multiple instances have the same HOSTNAME. Please set another ENV variable that is uniq. For example, you could set and ENV variable named MYSTRING to be
# export MYSTRING=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
# and you would run pyetl with export UNIQ_ENV="MYSTRING" instead. This ensures a lack of clobbering of files. 
export UNIQ_ENV="HOSTNAME"

# This is where you want to write the parquet files.  /app/data should be volume mounted outside your container, and then next level should be your table name
export TABLE_BASE="/app/data/mytable"

# This is the loop timeout, so that when this time is reached, it will cause the loop to occur (checking for older files etc)
export LOOP_TIMEOUT="5.0"

# If a partition was appended created or appended too, and there just any more data. pyetl keeps track of the last write. Of course if a file gets over FILEMAXSIZE it will merge but sometimes a partition will just be written without
# merging. So let's say you had a file with 255mb of data and you set your FILEMAXSIZE to 256mb.  Well if you have written to the partition but have not seen any writes for 600 seconds, then merge it so you do not have lots of row groups
export PARTMAXAGE=10

# This is the field in your json data that will be used to directory partition the fields. So if you provide a path  of /app/data/mytable, then the values of this field will be the directory
# names and all records with the that field having a X value will be written to X directory
export PARTITION_FIELD="day"

# Write to the live output directory (This can cause query errors, but you get faster access to data) If set to 0 then it will write to the tmp directory until the file is closed, then move to main dir. 
export WRITE_LIVE=0

# Does the data have NULLs? See fastparquet docs for details
export HAS_NULLS=0

# This is where tmp files our written during a merge.  Having the Preceding . keeps tools like Apache drill from querying it
export TMP_PART_DIR=".tmp"

export REMOVE_FIELDS_ON_FAIL=0 # Set to 1 to remove fields, starting left to right in the variable REMOVE_FIELDS to see if the json parsing works
export REMOVE_FIELDS="col1,col2" # If REMOVE_FIELDS_ON_FAIL is set to 1, and there is an error, the parser will remove the data from fields left to right, once json conversion works, it will break and move on (so not all fields may be removed)


# Turn on verbose logging.  For Silence export DEBUG=0
export DEBUG=1

# Run Py ETL!
python3 -u /app/code/pyparq.py

