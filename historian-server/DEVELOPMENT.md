# TESTS

## Architecture resources dans integration-test

* historian-conf : contient la conf pour la gateway lancer par le docker-compose 'docker-compose-for-grafana-tests.yml'
* http : Contient des requêtes http et réponse pour les tests d'intégration
* scripts : contient des script pour injecter des données dans la gateway lancer par le docker-compose 'docker-compose-for-grafana-tests.yml'
* solr : contient la conf pour les collection solr nécessair a la gateway, c'est utiliser pour les tests d'intégrations ET
  par le docker-compose 'docker-compose-for-grafana-tests.yml'
* docker-compose-for-grafana-tests.yml : Utiliser pour lancer la gateway avec des données d'exemple. A utiliser principalement 
pour tester du développement sur grafana avec des données et une gateway fonctionnel, comme des plugins de datasource ou de panel.
* docker-compose-test.yml : Utilisé pour lancer les tests d'intégrations

## Run gateway with docker-compose

go into the resources and then run the docker-compose file :

```shell script
cd ./src/integration-test/resources
docker-compose -f docker-compose-for-grafana-tests.yml up -d
```

Then to add the historian datasource to your grafana instance, you need to have set up your grafana environment (in another project).
https://grafana.com/docs/grafana/latest/plugins/developing/development/

In grafana project go to devenv and add this to the datasource yaml file :

```shell script
vim ./devenv/datasource.yaml
```

add this to the end of the file :

```yaml
  - name: Hurence-Historian
    type: grafana-hurence-historian-datasource
    access: proxy
    url: http://localhost:8080/api/grafana
```
  
Run :

```shell script
cd ./devenv
bash ./setup.sh
```

This will create all datasource in datasource.yaml file. Including the historian one that we just added.

Then you can add any custom plugin you want in ./data/plugins folder to test them !
Current hurence plugins projects :
* https://github.com/Hurence/grafana-historian-datasource
* https://github.com/Hurence/grafana-annotation-panel

I advised you to git clone those project inside a grafana-plugin folder that you will sim link to ./data/plugins of your grafana setup.
This way you can work on the plugin project, compile it, and then restart backend grafana to test your modifications.



