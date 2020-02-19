#!/usr/bin/env bash

echo "running create collection"
# Upload in zookepper
#bin/solr zk -help
bin/solr zk upconfig -n historian -d /opt/solr/configsets/historian
bin/solr zk upconfig -n annotation -d /opt/solr/configsets/annotation

curl 'http://solr1:8983/solr/admin/collections?action=CREATE&name=historian&numShards=1&collection.configName=historian'
curl 'http://solr1:8983/solr/admin/collections?action=CREATE&name=annotation&numShards=1&collection.configName=annotation'