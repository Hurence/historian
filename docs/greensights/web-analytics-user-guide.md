---
layout: page
title: Web analytics user guide
---

The following guide will help you settings up a new account to use 


## Preliminary config steps

[Créer et gérer des comptes de service](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

https://console.cloud.google.com/projectselector2/iam-admin/serviceaccounts?supportedpurview=project

you now get a json file like 

```json
{
  "type": "service_account",
  "project_id": "historian",
  "private_key_id": "e7bd4a805872a8842f20442",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEIBADANBgkqhkiG9w0BAQAgEAAoIBAQDG1MZUQhuAbJAk\nyc4RXnAuKbmfBZTnV5Qw9LGdOH6Bvs9X50KyMEgJBd+EZRDJ4RvOltfvZ3useZbs\n+TqnvZ1LVQazONw85AloOji5NoS2TAu9mU5gixpvBJBkCu0oYLbQMKVt6hJyu/aK\nrd6wFTImxiNlFSPj84bk7plHTyZvWmiXnPAYW3uO6Rbin9qdeE\nuDIeurSDyrhm6RHrTzmoLAhpt8spAoNOZaAvm6MMQM7ivgsCkzJFiKQ7hQ+1sMA3\n5QH+Z1/CO5Kmv6mXkEnjCUYJQw==\n-----END PRIVATE KEY-----\n",
  "client_email": "greensights@historian.iam.gserviceaccount.com",
  "client_id": "111934842662",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/greensights.iam.gserviceaccount.com"
}


```


## Setup historian and web services

We need two collection, one for historian data
```bash
curl --location --request POST "http://${SOLR_HOST}:${SOLR_PORT}/v2/c" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "create": {
        "name": "${HISTORIAN_COLLECTION}",
        "config": "historian",
        "numShards": 3,
        "replicationFactor": 2,
        "maxShardsPerNode": 6
    }
}'
```

the other for greensights entities
```bash
curl --location --request POST "http://${SOLR_HOST}:${SOLR_PORT}/v2/c" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "create": {
        "name": "${GRS_COLLECTION}",
        "config": "_default",
        "numShards": 1,
        "replicationFactor": 2,
        "maxShardsPerNode": 3
    }
}'
```

based on that we can launch our greensights server instance

```bash
nohup java -jar lib/greensights-server-${GRS_VERSION}.jar --server.port=${GRS_PORT} --greensights.scraper.jsonKeyFile=${GS_JSON_KEYFILE_PATH} --historian.solr.greensights.collection=${GRS_COLLECTION} --historian.solr.collection=${HISTORIAN_COLLECTION} >> log/greensights-server-bl-evolution.log  2>&1 &
```

note that the historian server conf must be set accordingly

```json
{
  "web.verticles.instance.number": 2,
  "historian.verticles.instance.number": 4,
  "http_server" : {
    "host": "0.0.0.0",
    "port" : ${HISTORIAN_PORT},
    "historian.address": "historian",
    "debug": false,
    "upload_directory" : "/tmp/historian",
    "max_data_points_maximum_allowed" : 50000
  },
  "historian.metric_name_lookup.csv_file.path": "./synonyms.csv",
  "historian.metric_name_lookup.csv_file.separator": ";",
  "historian.metric_name_lookup.enabled": false,
  "historian": {
    "schema_version": "VERSION_1",
    "address" : "historian",
    "limit_number_of_point_before_using_pre_agg" : 50000,
    "limit_number_of_chunks_before_using_solr_partition" : 50000,
    "api": {
      "grafana": {
        "search" : {
          "default_size": 100
        }
      }
    },
    "solr" : {
      "use_zookeeper": true,
      "zookeeper_urls": ["localhost:9983"],
      "zookeeper_chroot" : null,
      "stream_url" : "http://localhost:8983/solr/${GRS_COLLECTION}",
      "chunk_collection": "${GRS_COLLECTION}",
      "annotation_collection": "annotation",
      "sleep_milli_between_connection_attempt" : 10000,
      "number_of_connection_attempt" : 3,
      "urls" : null,
      "connection_timeout" : 10000,
      "socket_timeout": 60000
    }
  }
}
```


## Firewall settings

déjà dans un premier temps si on souhaite sécuriser un peu plus fermement le serveur je veux bien que vous me transmettiez une liste d’adresses IP à autoriser par le firewall comme ça on sera tranquilles.

pour le moment les ports suivants (3000 et 8082) sont ouverts de manière publique et seul grafana est protégé par login/mot de passe. Le serveur du port 8082 est votre web service dédié qui vous permet de lancer les calculs d'empreinte énergétique.
[grafana](http://ns3134986.ip-51-77-132.eu:3000)
[greensights_server_bl_evolution](http://ns3134986.ip-51-77-132.eu:8082)

vous disposez de votre propre collection (table) de données dans SolR, de votre propre service logiciel pour y accéder, de votre propre compte de service analytics, comme ça les données sont bien cloisonnées.
Un email de service qu'il faut autoriser dans votre compte Google Analytics
greensights-bl-evolution@historian-329415.iam.gserviceaccount.com

pour ce faire il suffit de sélectionner dans GA le compte analytics et la propriété que vous souhaitez instrumenter puis cliquer sur "Gestion des accès au compte" et y rajouter l'email de service ci-dessus en tant que simple lecteur sans accès aux métrique de coût, comme je l'ai fait ci-dessous pour mon account historian (qui apparaîtra donc dans vos données)

Notez que ce compte n'est utilisé que par vous et que vous pouvez révoquer à tout moment les autorisations.

## Report computing request

à partir de là vous pourrez lancer des requêtes d'analyse via une API REST

```bash
curl --location --request POST 'http://ns3134986.ip-51-77-132.eu:8082/api/v1/web-analytics/compute' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "startDate": "2022-01-22",
        "endDate": "2022-01-25",
        "doSaveMeasures": true,
        "doSaveMetrics": true,
        "doComputeDayByDay": true,
        "accountFilters": [],
        "rootUrlFilters": [ "https://www.hurence.com"] 
    }'
```

qui devraient vous renvoyer des rapports du type
```json
[{
    "rootUrl": "https://hurence.github.io/historian/",
    "pagePath": null,
    "startDate": "2022-01-22",
    "endDate": "2022-01-23",
    "transferredBytes": 2470480,
    "pageViews": 5,
    "avgTimeOnPageInSec": 48,
    "avgPageSizeInBytes": 494096,
    "energyImpactInKwh": 0.0016420299833333333,
    "energyImpactByPageInKwh": 3.2840599666666666E-4,
    "co2EqInKg": 5.7471049416666674E-5,
    "co2EqByPageInKg": 1.1494209883333334E-5,
    "labels": {
    "root_url": "https://hurence.github.io/historian/",
    "scope": "site"
}, ...]
```


cette requête (qui peut être longue surtout lors des premiers lancements où le cache n'est pas encore rempli) fait 3 opérations :

    elle va chercher dans tous les comptes Google Analytics les propriétés ou vues autorisées et demande le trafic (pageViews et avgTimeOnPage) pour l'intervalle temps demandé.
    pour chaque page le moteur va analyser son contenu web (si cela n'a pas déjà été fait récemment) et stocker le résultat dans solr
    ensuite on calcule le bilan carbone via le one byte model (ratio taille de page / temps passé / type de device /pays d'origine) qui est stocké lui-aussi dans solr

Attention :

        Veillez à ne pas mettre un intervalle de temps trop grand entre startDate et endDate (quelques jours suffisent) pour que la requête ne soit pas trop longue
        La requête va par défaut prendre les données analytics de tous les sites pour un account autorisé, ce qui peut être conséquent, il est possible de spécifier un filtre pour exclure explicitement certains sites du calcul, par exemple : "rootUrlFilters": [ "https://www.hurence.com"]

## Consume data trough Grafana

pour accéder aux données stockées dans Solr on va passer par Grafana

http://ns3134986.ip-51-77-132.eu:3000/d/VXjxS-0nz/power-usage-web-analytics?orgId=2&refresh=1m



vous pouvez créer vos propres dashboards et explorer les données comme bon vous semble, les métriques suivantes étant disponibles

"avg_time_on_page_sec" "co2_eq_kg" "energy_impact_kwh" "page_views" "avg_page_size_bytes" "co2_eq_by_page_kg" "energy_impact_by_page_kwh" "transferred_bytes" "page_size_bytes"


je vous donnerai un peu plus tard l'accès aux données d'analyse des pages web

```json
{ "id":"https://www.hurence.com/fr-consulting/fr-designing-big-data-infrastructures/", "doc_type_s":"webpage_analysis", "page_size_in_bytes_l":1512838, "page_nodes_i":281, "num_requests_i":27, "ecoindex_grade_s":"C", "ecoindex_score_f":62.0, "ecoindex_ges_f":1.76, "ecoindex_water_f":2.64, "download_duration_l":1979, "computation_date_dt":"2022-02-07T13:39:11.392Z", "_version_":1724111812198137856},
```

