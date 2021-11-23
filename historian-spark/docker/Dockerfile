# docker build --rm -t hurence/historian-spark  .
# docker tag hurence/historian-spark:latest hurence/historian-spark:1.3.8

FROM anapsix/alpine-java:8_jdk_nashorn
ARG spark_version="2.3.2"
ARG historian_version="1.3.9"

MAINTAINER hurence

RUN apk add --update curl docker jq coreutils procps vim

VOLUME ["/spark"]

# Spark
RUN echo "https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.7.tgz"
RUN curl -s https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.7.tgz | tar -xz -C /opt/
RUN cd /opt && ln -s spark-${spark_version}-bin-hadoop2.7 spark
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin


# Historian
ARG BUNDLE_FILE=historian-${historian_version}-bin.tgz
COPY ${BUNDLE_FILE} /opt
RUN cd /opt && tar xzf ${BUNDLE_FILE}  && ln -s historian-${historian_version} historian
ENV HISTORIAN_HOME /opt/historian
#RUN tar -xz /opt/${BUNDLE_FILE}

WORKDIR $HISTORIAN_HOME/
ENTRYPOINT ["bin/historian-compactor.sh", "start", "-c", "/opt/historian/conf/compactor-config.yaml", "-s", "/opt/spark"]
