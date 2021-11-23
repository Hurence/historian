FROM vertx/vertx3

ARG historian_version="1.3.9"
ENV HISTORIAN_HOME /opt/historian/
EXPOSE 8080

COPY ./historian-server-${historian_version}-fat.jar $HISTORIAN_HOME
COPY ./config.json $HISTORIAN_HOME/conf/
COPY ./log4j.properties $HISTORIAN_HOME/
COPY ./historian-configset.zip $HISTORIAN_HOME/

WORKDIR $HISTORIAN_HOME

ENTRYPOINT ["java", "-Dlog4j.configuration=file:log4j.properties", "-jar", "./historian-server-${historian_version}-fat.jar", "-conf" , "./conf/config.json"]