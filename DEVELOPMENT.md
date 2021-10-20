# Presentation

This project is composed of several module : 

* gateway : This is the Rest api of the historian
* grafana-historian-datasource : This is a grafana datasource plugin that can communicate with the gateway and display data on grafana dashboards.
* loader : TODO
* logisland-timeseries : TODO
* timeseries : TODO

## Historian server

The historian server is implemented using vertx. 
It serves an API that the grafana-historian-datasource needs to interact with grafana.
So those two modules are tightly coupled by the Rest api.

## grafana-historian-datasource

This is implemented using javascript. We recommend you to directly develop this plugin in its own
 [repo](https://github.com/Hurence/grafana-historian-datasource).
You develop this plugin, implement unit tests as indicated in the project. Once you have push your changes in the datasource project.

## install plugin in grafana

You just need to copy the ./dist folder of the plugin into the plugin folder of your grafana instances. Look at your
grafana.ini file, by default the path is ./data/plugins/

SO you could do something like
 
 ``` shell script
cp -r ./grafana-historian-datasource ${GRAFANA_HOME}/data/plugins/
```

## How to test the grafana plugin ?

To test the grafana plugin we recommend using the grafana Dev environment. We [forked](https://github.com/Hurence/grafana) the grafana project for this purpose.
Clone this project. We included the grafana-historian-datasource project as a sub module of our project so that we can test it.
The sub project is localized at ./data/plugins/grafana-historian-datasource of the grafana project.

We recommend using visual studio code for developping grafana plugins. You can check grafana development documentation too.
In particular to build and run grafana in you development environment you can follow this [guide](https://github.com/grafana/grafana/blob/master/contribute/developer-guide.md) or this [one](https://medium.com/@ivanahuckova/how-to-contribute-to-grafana-as-junior-dev-c01fe3064502).

## Modify documentation

Go to ./docs folder and modify .ad files. Then run mvn clean install in ./docs folder then commit newly generated pdf.
If it is a new file you may add it in README.MD. You should modify documentation on the corresponding branch release. 

## Build project
This section is mainly for developers as it will guive some insight about compiling testing the framework.
Hurence Historian is Open Source, distributed as Apache 2.0 licence and the source repository is hosted on github at [https://github.com/Hurence/historian](https://github.com/Hurence/historian)

Run the following command in the root directory of historian source checkout.

    git clone git@github.com:Hurence/historian.git
    cd historian
    mvn clean install -DskipTests -Pbuild-integration-tests
    
 To run integration tests
 
    mvn install -Pintegration-tests
    
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
    
## Good practices

### Logs

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

  
### Tests

Unit test should be implemented inside src/test directory and suffixed with "Test".
Integration tests should be put into src/integration-test and be suffixed with "IT" so 
that it is not run as Unit tests !

