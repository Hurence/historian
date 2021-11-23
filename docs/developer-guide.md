---
layout: page
title: Developer guide
---

This project is composed of several module : 

* gateway : This is the Rest api of the historian
* grafana-historian-datasource : This is a grafana datasource plugin that can communicate with the gateway and display data on grafana dashboards.
* loader : TODO
* logisland-timeseries : TODO
* timeseries : TODO


### Build
You juste need a good IDE, JDK 8+ and maven

    mvn clean install

### Historian server

The historian server is implemented using vertx. 
It serves an API that the grafana-historian-datasource needs to interact with grafana.
So those two modules are tightly coupled by the Rest api.

### Modify documentation

Go to ./docs folder and modify .ad files. Then run mvn clean install in ./docs folder then commit newly generated pdf.
If it is a new file you may add it in README.MD. You should modify documentation on the corresponding branch release. 

## Build project
This section is mainly for developers as it will guive some insight about compiling testing the framework.
Hurence Historian is Open Source, distributed as Apache 2.0 licence and the source repository is hosted on github at [https://github.com/Hurence/historian](https://github.com/Hurence/historian)

Run the following command in the root directory of historian source checkout.

    git clone git@github.com:Hurence/historian.git
    cd historian
    mvn clean install -DskipTests -Pbuild-integration-tests

## Release process

Create release branch :

```bash
git hf release start vx.x.x
```

Update maven version :

```bash
mvn versions:set -DnewVersion=x.x.x
# commit if ok
mvn versions:commit
```

Ensure all tests are passing (even integ ones)

```bash
mvn clean install
mvn -Pintegration-tests -q clean verify
```


verify modification and commit them.

Test the standalone install in ./assembly/target/historian-X.X.X-install.tgz.
Here the process :

Extract it somewhere (we will use /tmp here (be sure to have enough disk)).

```bash
mkdir -p /tmp/historian && tar -xf ./assembly/target/historian-*-install.tgz -C /tmp/historian
```
run the script install :

```bash
bash ./install.sh
```
    
## Logging

* Never use println
* Never use show() (Dataframe) or conditionnaly, for exemple :
```
 if (logger.isDebugEnabled) {
        ack08.show()
 }
```
* Log dependencies should only be set on root pom but never in childs. This way log management is the same in every module.
In our case we use :
  * slf4j-api
  * slf4j-log4j12
  * log4j
If using slf4j, you only use an implementation. Here we use log4j, the dependence slf4j-log4j12 allow
us to use log4j.properties files to configure log level. We have 2 log4j.properties per module in resources folders,
one in main and one in test folders.
  
We added the enforcer plugin to ensure that all slf4j implementation embedded by transitive dependencies are excluded.
If not you will get an error message and you have to exclude the conflicting dependency.


