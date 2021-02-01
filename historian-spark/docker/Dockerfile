# docker build --rm -t hurence/historian  .
# docker tag hurence/historian:latest hurence/historian:2.4.3


FROM anapsix/alpine-java:8_jdk_nashorn
ARG spark_version="2.3.2"

MAINTAINER hurence

RUN apk add --update unzip wget curl docker jq coreutils procps vim

VOLUME ["/spark"]

# Spark
RUN echo "https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.7.tgz"
RUN curl -s https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.7.tgz | tar -xz -C /opt/
RUN cd /opt && ln -s spark-${spark_version}-bin-hadoop2.7 spark
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin
WORKDIR $SPARK_HOME/

# Historian
