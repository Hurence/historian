# Presentation

This project is composed of several module : 

* gateway : This is the Rest api of the historian
* grafana-historian-datasource : This is a grafana datasource plugin that can communicate with the gateway and display data on grafana dashboards.
* loader : TODO
* logisland-timeseries : TODO
* timeseries : TODO

# gateway

The gateway is implemented using vertx. It serves an API that the grafana-historian-datasource needs to interact with grafana.
SO those two modules are tightly coupled by the Rest api.

# grafana-historian-datasource

This is implemented using javascript. We recommend you to directly develop this plugin in its own
 [repo](https://github.com/Hurence/grafana-historian-datasource).
You develop this plugin, implement unit tests as indicated in the project. Once you have push your changes in the datasource project.
You can just update the submodule in this project by typing :

```
git submodule update --remote
```

It will automatically pull the last commit of the project in this repository.
Then do not forget to commit the differences !

## install plugin in grafana

You just need to copy the ./dist folder of the plugin into the plugin folder of your grafana instances. Look at your
grafana.ini file, by default the path is ./data/plugins/

SO you could do something like
 
 ``` shell script
cp -r ./grafana-historian-dataosurce ${GRAFANA_HOME}/data/plugins/
```

## How to test the grafana plugin ?

To test the grafana plugin we recommend using the grafana Dev environment. We [forked](https://github.com/Hurence/grafana) the grafana project for this purpose.
Clone this project. We included the grafana-historian-datasource project as a sub module of our project so that we can test it.
The sub project is localized at ./data/plugins/grafana-historian-datasource of the grafana project.

We recommend using visual studio code for developping grafana plugins. You can check grafana development documentation too.
In particular to build and run grafana in you devlopment environment you can follow this [guide](https://github.com/grafana/grafana/blob/master/contribute/developer-guide.md) or this [one](https://medium.com/@ivanahuckova/how-to-contribute-to-grafana-as-junior-dev-c01fe3064502).


Once you successfully ran those guides you can run
```
git submodule init
git submodule update
```
to retrieve the plugin






