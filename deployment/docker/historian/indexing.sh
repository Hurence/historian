#!/usr/bin/env bash

curl "http://localhost:8983/solr/admin/collections?action=CREATE&name=mycoll2&numShards=2&replicationFactor=1"
for i in {1..10000}
  do echo $i; curl "http://localhost:8983/solr/mycoll2/update" -X POST -d "[{'id':'id$i'}, {'id':'myid$i'}]" -H "Content-type: application/json"
done
