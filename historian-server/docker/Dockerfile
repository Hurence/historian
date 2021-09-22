FROM vertx/vertx3

ENV HISTORIAN_HOME /opt/historian/
EXPOSE 8080

COPY ./historian-server-1.3.8-fat.jar $HISTORIAN_HOME
COPY ./config.json $HISTORIAN_HOME/conf/
COPY ./log4j.properties $HISTORIAN_HOME/
COPY ./historian-configset.zip $HISTORIAN_HOME/

WORKDIR $HISTORIAN_HOME

ENTRYPOINT ["java", "-Dlog4j.configuration=file:log4j.properties", "-jar", "./historian-server-1.3.8-fat.jar", "-conf" , "./conf/config.json"]