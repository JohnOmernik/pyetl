FROM ubuntu:latest
MAINTAINER John Omernik Mandolinplayer@gmail.com


RUN apt-get update && apt-get install -y git wget syslinux libevent-pthreads-2.0-5 nano  && apt-get clean && apt-get autoremove -y

# LLVM String Added no matter what is selected for output
RUN wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-3.9 main" > /etc/apt/sources.list.d/LLVM.list
RUN echo "deb-src http://apt.llvm.org/xenial/ llvm-toolchain-xenial-3.9 main" >> /etc/apt/sources.list.d/LLVM.list
ENV LLVM_CONFIG="/usr/lib/llvm-3.9/bin/llvm-config"

RUN mkdir -p /app/code && mkdir -p /app/data

RUN apt-get update && apt-get install -y python python-dev python-setuptools python-pip python3 python3-dev python3-setuptools python3-pip build-essential openjdk-8-jdk zlib1g-dev libssl-dev libsasl2-dev liblz4-dev libsnappy1v5 libsnappy-dev liblzo2-2 liblzo2-dev  && apt-get clean && apt-get autoremove -y

ENV C_INCLUDE_PATH=/opt/mapr/include
ENV LIBRARY_PATH=/opt/mapr/lib
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV LD_LIBRARY_PATH=/opt/mapr/lib:$JAVA_HOME/jre/lib/amd64/server

RUN apt-get update && apt-get install -y clang-3.9 lldb-3.9 && apt-get clean && apt-get autoremove -y

ENV PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server
ENV LD_PRELOAD=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so





RUN wget http://package.mapr.com/releases/v5.2.1/ubuntu//mapr-client-5.2.1.42646.GA-1.amd64.deb && wget http://package.mapr.com/releases/MEP/MEP-3.0/ubuntu//mapr-librdkafka_0.9.1.201703301726_all.deb && dpkg -i mapr-client-5.2.1.42646.GA-1.amd64.deb && dpkg -i mapr-librdkafka_0.9.1.201703301726_all.deb && rm mapr-client-5.2.1.42646.GA-1.amd64.deb && rm mapr-librdkafka_0.9.1.201703301726_all.deb && ldconfig


RUN git clone https://github.com/mkmoisen/pychbase.git && cd pychbase && sed -i "s/library_dirs=library_dirs,/library_dirs=library_dirs,extra_compile_args=['-fpermissive'],/g" setup.py && python setup.py install && cd .. && rm -rf pychbase


RUN pip install python-snappy python-lzo brotli kazoo requests pytest
RUN pip3 install python-snappy python-lzo brotli kazoo requests pytest
RUN pip install confluent-kafka confluent-kafka[avro]
RUN pip3 install confluent-kafka confluent-kafka[avro]

RUN pip3 install numpy pandas cython numba
RUN pip3 install git+https://github.com/dask/fastparquet


ADD pyetl.py /app/code/

CMD ["/bin/bash"]


