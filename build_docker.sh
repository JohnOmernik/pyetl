#!/bin/bash

#
# Pyetl Docker file to build for use with multiple Queues and Backends
#
# Supported Queues:
# Apache Kafka
# MapR Streams
#
# Supported Outputs:
# json  to posix mount Point
# parquet to posix mount point
# maprdb
#
# Untested Outputs
# hbase
#
# Todo outputs:
# json to hdfs
# parquet to hdfs
#
# Do consider Kafka Connect for your ETL needs.
#

PARQ_OUT="1" # Include Fast Parquet outputs
MAPRDB_OUT="1" # Include library for MaprDB Output
STREAMS_IN="1" # Use MapR Librdkafka to attach to streams (if 0 then use Apache Kafka librdkafka)

APT_BASE="git wget syslinux libevent-pthreads-2.0-5 nano"
APT_PYTHON2="python python-dev python-setuptools python-pip"
APT_PYTHON3="python3 python3-dev python3-setuptools python3-pip"
APT_BUILD="build-essential"
APT_JAVA8="openjdk-8-jdk"
APT_BUILD_LIBS="zlib1g-dev libssl-dev libsasl2-dev liblz4-dev libsnappy1v5 libsnappy-dev liblzo2-2 liblzo2-dev"
APT_CLANG="clang-3.9 lldb-3.9"

PIP_BASE="python-snappy python-lzo brotli kazoo requests pytest"
PIP_CONFLUENT="confluent-kafka confluent-kafka[avro]"
PIP_FASTPARQ_REQ="numpy pandas cython numba"
PIP_FASTPARQ="git+https://github.com/dask/fastparquet"

MAPR_CLIENT_BASE="http://package.mapr.com/releases/v5.2.1/ubuntu/"
MAPR_CLIENT_FILE="mapr-client-5.2.1.42646.GA-1.amd64.deb"
MAPR_LIBRDKAFKA_BASE="http://package.mapr.com/releases/MEP/MEP-3.0/ubuntu/"
MAPR_LIBRDKAFKA_FILE="mapr-librdkafka_0.9.1.201703301726_all.deb"

MAPR_CLIENT="RUN wget ${MAPR_CLIENT_BASE}/${MAPR_CLIENT_FILE} && wget ${MAPR_LIBRDKAFKA_BASE}/${MAPR_LIBRDKAFKA_FILE} && dpkg -i ${MAPR_CLIENT_FILE} && dpkg -i ${MAPR_LIBRDKAFKA_FILE} && rm ${MAPR_CLIENT_FILE} && rm ${MAPR_LIBRDKAFKA_FILE} && ldconfig"

PIP_BASE_CMD="RUN pip install $PIP_BASE"
PIP_KAFKA_CMD="RUN pip install $PIP_CONFLUENT"
PIP3_BASE_CMD="RUN pip3 install $PIP_BASE"
PIP3_KAFKA_CMD="RUN pip3 install $PIP_CONFLUENT"

INST_MAPR=""$'\n'
INST_KAFKA_LIBRDKAFKA=""$'\n'

APT_INIT_STR="${APT_BASE}"
APT_INST_STR="${APT_PYTHON2} ${APT_PYTHON3} ${APT_BUILD} ${APT_JAVA8} ${APT_BUILD_LIBS}"

if [ "$PARQ_OUT" == "1" ]; then
    PIP_PARQ="RUN pip3 install $PIP_FASTPARQ_REQ"$'\n'
    PIP_PARQ="${PIP_PARQ}RUN pip3 install $PIP_FASTPARQ"$'\n'
    APT_PARQ_STR="RUN apt-get update && apt-get install -y $APT_CLANG && apt-get clean && apt-get autoremove -y"
else
    PIP_PARQ=""$'\n'
    APT_PARQ_STR=""$'\n'
fi

if [ "$MAPRDB_OUT" == "1" ]; then
    INST_MAPR="$MAPR_CLIENT"$'\n'
    PIP_MAPRDB="RUN git clone https://github.com/mkmoisen/pychbase.git && cd pychbase && sed -i \"s/library_dirs=library_dirs,/library_dirs=library_dirs,extra_compile_args=['-fpermissive'],/g\" setup.py && python setup.py install && cd .. && rm -rf pychbase"$'\n'
    MAPRDB_ENV="ENV PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server"$'\n'
    MAPRDB_ENV="${MAPRDB_ENV}ENV LD_PRELOAD=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so"$'\n'
else
    PIP_MARDB=""$'\n'
    MAPRDB_ENV=""$'\n'
fi



if [ "$STREAMS_IN" == "1" ] || [ "$MAPRDB_OUT" == "1" ]; then
    INST_MAPR="$MAPR_CLIENT"$'\n'
else
    INST_KAFKA_LIBRDKAFKA="RUN wget https://github.com/edenhill/librdkafka/archive/v0.9.4.tar.gz && tar zxf v0.9.4.tar.gz && cd librdkafka-0.9.4 && ./configure && make && make install && ldconfig && cd .. && rm -rf librdkafka-0.9.4 && rm v0.9.4.tar.gz"
fi



cat  > Dockerfile << EOD
FROM ubuntu:latest
MAINTAINER John Omernik Mandolinplayer@gmail.com


RUN apt-get update && apt-get install -y $APT_INIT_STR  && apt-get clean && apt-get autoremove -y

# LLVM String Added no matter what is selected for output
RUN wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-3.9 main" > /etc/apt/sources.list.d/LLVM.list
RUN echo "deb-src http://apt.llvm.org/xenial/ llvm-toolchain-xenial-3.9 main" >> /etc/apt/sources.list.d/LLVM.list
ENV LLVM_CONFIG="/usr/lib/llvm-3.9/bin/llvm-config"

RUN mkdir -p /app/code && mkdir -p /app/data

RUN apt-get update && apt-get install -y $APT_INST_STR  && apt-get clean && apt-get autoremove -y

ENV C_INCLUDE_PATH=/opt/mapr/include
ENV LIBRARY_PATH=/opt/mapr/lib
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV LD_LIBRARY_PATH=/opt/mapr/lib:\$JAVA_HOME/jre/lib/amd64/server

$APT_PARQ_STR

$MAPRDB_ENV

$INST_KAFKA_LIBRDKAFKA

$INST_MAPR

$PIP_MAPRDB

$PIP_BASE_CMD
$PIP3_BASE_CMD
$PIP_KAFKA_CMD
$PIP3_KAFKA_CMD

$PIP_PARQ

ADD pyetl.py /app/code/

CMD ["/bin/bash"]


EOD



