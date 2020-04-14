# Logisland Data Historian

timeseries big data analytics tools


## Setup

make a workspace directory called historian for example. we'll refer to it as `$HISTORIAN_HOME`

create a directory to store your data

    mkdir $HISTORIAN_HOME/data

### Install SolR
To follow along with this tutorial, you will need to unpack the following solr archive : https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz into your `$HISTORIAN_HOME` directory

then start 2 solr cores locally

    cd $HISTORIAN_HOME/solr-8.2.0

    bin/solr start -cloud -s ../data/solr/node1  -p 8983
    bin/solr start -cloud -s ../data/solr/node2/  -p 7574 -z localhost:9983
    

### Install Historian
you will need to unpack the following solr archive : https://github.com/Hurence/historian/releases/download/v1.3.4/historian-1.3.4-SNAPSHOT.tgz  into your `$HISTORIAN_HOME` directory


you can now create the solr schema for the SolR historian collection

    cd $HISTORIAN_HOME/historian-1.3.4-SNAPSHOT
    bin/create-historian-collection.sh
    
and launch the historian REST server

    bin/historian-server.sh
    

    
### Install Spark


###     
    
    
### Stop your services

when you're done you can stop your SolR cores
    
    bin/solr stop -all
    
if you want to reset manually your data
    
    rm -r ../data/solr/node1/historian_shard1_replica_n1/ ../data/solr/node1/zoo_data/ ../data/solr/node2/historian_shard2_replica_n2/
    
    
# First time cloning project

This project include git sub modules. That's why you need to clone this project with the option --recurse-submodules.

``
git clone --recurse-submodules url
``

If you already cloned this project you can do instead :


```
git submodule init
```

```
git submodule update
```

For more information on git sub modules please see git documentation : https://git-scm.com/book/fr/v2/Utilitaires-Git-Sous-modules

# Modify sub modules

As explained in sub module documentation, to update it you have to go inside the module.
Commit your change.
 
You can rebase with remote repository at any moment :

```
git submodule update --remote --rebase
```

If you do not specify --rebase nor --merge, your changes will not be applied, instead it will swith you with the origin branch.
Your work would still be available on your local branch. Or if it was conflicting it would warn you. 

## Publishing Submodule Changes

Either run if you want the push to fail if submodule are not up to date
```
git push --recurse-submodules=check
```

Either run if you want the push all your work event what you did in the submodule (this will directly update git project of the sub module)
```
git push --recurse-submodules=on-demand
```

# Build project

The run this command in root of this project

```
mvn clean install -DskipTests
```

# Install datasource plugin in your grafana instance

## Requirement

Run this command to be sure to be up to date. 

```
git submodule update
```

If you modified the plugin, working on his own repository and you want to upgrade the datasource in this project run this instead :

```
git submodule update --remote
```

And do not forget to commit the results so that others will got the updated version as well.

## install plugin

You just need to copy the plugin folder **./grafana-historian-dataosurce** folder of the plugin into the plugin folder of your grafana instances.
Look at your grafana.ini file, by default the path is **./data/plugins/**.

So you could do something like
 
 ``` shell script
cp -r ./grafana-historian-dataosurce ${GRAFANA_HOME}/data/plugins/
```

You need to restart your grafana server so that the changes are taking in account.

# Development

Please see our documentation [here](DEVELOPMENT.md)



weekly team 10/02/2020

- loader : 
    - BENOIT / PR du chargement des données coservit en mode générique
    - TOM / review PR & merge
    - TOM / chargement 1 mois de data pour démo
- gateway : 
    - GREG / merge PR du filtrage du nom des métriques
    - FEIZ / implem de la recherche des annotations
- analytic :
    - MEJD / intég de la fonction de seuil SAX dans la lib timeseries + unit tests
    - MEJD / integ de la lib grammar viz
    - TOM / utilisation de la fonction seuils SAX pour indexer des annotations





# Install

## Solr

Begin by unzipping the Solr release and changing your working directory to the subdirectory where Solr was installed. For example, with a shell in UNIX, Cygwin, or MacOS:

get solr from  `https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.zip`


~$ ls solr*
solr-8.2.0.zip

~$ unzip -q solr-8.2.0.zip

~$ cd solr-8.2.0/

If you’d like to know more about Solr’s directory layout before moving to the first exercise, see the section Directory Layout for details.


Choose any available port for each node; the default for the first node is 8983 and 7574 for the second node. The script will start each node in order and show you the command it uses to start the server, such as:

bin/solr start -cloud -s ../data/solr/node1  -p 8983
bin/solr start -cloud -s ../data/solr/node2/  -p 7574 -z localhost:9983


bin/solr stop -all


## Historian gateway

unpack historian-1.0.0.zip

run it

    cd historian-1.0.0
    bin/historian-gateway.sh



```json
curl --location --request POST 'http://localhost:8080/api/grafana/query' \
--header 'Content-Type: application/json' \
--data-raw '{
  "panelId": 1,
  "range": {
    "from": "2020-03-01T00:00:00.000Z",
    "to": "2020-03-01T23:59:59.000Z"
  },
  "interval": "30s",
  "intervalMs": 30000,
  "targets": [
    {
      "target": "\"U81.PT232.F_CV\"",
      "refId": "U81.PT232.F_CV",
      "type": "timeserie"
    }
  ],
  "format": "json",
  "maxDataPoints": 550
}'
```






Grafana


Install Grafana for your platform as described here : `https://grafana.com/docs/grafana/latest/installation/requirements/ `
Install Json Simple Datasource as described here : 

https://grafana.com/grafana/plugins/grafana-simple-json-datasource


To install this plugin using the grafana-cli tool:

    sudo grafana-cli plugins install grafana-simple-json-datasource
    sudo service grafana-server restart
