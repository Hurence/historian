# delete the collection, if it already exists
curl "http://localhost:48983/solr/admin/collections?action=DELETE&name=mycoll2"

# create collection with one replica and add another replica (so that first)
# one becomes the leader
curl "http://localhost:48983/solr/admin/collections?action=CREATE&name=mycoll2&numShards=1&tlogReplicas=1&createNodeSet=solr1:8983_solr"
curl "http://localhost:48983/solr/admin/collections?action=ADDREPLICA&collection=mycoll2&shard=shard1&type=tlog"

# wait a bit for the added replica to become active
sleep 5

# set a low auto softcommit
#curl 'http://localhost:48983/solr/mycoll2/config' -H 'Content-type:application/json' -d '{"set-property" : {"updateHandler.autoCommit.maxTime" : 60000}}'
#curl 'http://localhost:48983/solr/mycoll2/config' -H 'Content-type:application/json' -d '{"set-property" : {"updateHandler.autoCommit.maxDocs" : 5000000}}'
#curl 'http://localhost:48983/solr/mycoll2/config' -H 'Content-type:application/json' -d '{"set-property" : {"updateHandler.autoSoftCommit.maxTime" : 60000}}'
curl 'http://localhost:48983/solr/mycoll2/config' -H 'Content-type:application/json' -d '{"set-property" : {"updateHandler.autoSoftCommit.maxDocs" : 250}}'

# print the cluster state (so that we know which node has the leader)
curl "http://localhost:48983/solr/admin/collections?action=CLUSTERSTATUS&collection=mycoll2&omitHeader=true" | jq ."cluster.collections.mycoll2.shards.shard1.replicas"

# add few docs
for i in {1..1000}
do curl -s "http://localhost:48983/solr/mycoll2/update" -X POST \
  -d "[{'id':'$i'}]" -H "Content-type: application/json" > /dev/null
done

# commit
curl "http://localhost:48983/solr/mycoll2/update?commit=true"

# add few docs
for i in {2001..3000}
do curl -s "http://localhost:48983/solr/mycoll2/update" -X POST \
  -d "[{'id':'$i'}]" -H "Content-type: application/json"  > /dev/null
done

# commit
curl "http://localhost:48983/solr/mycoll2/update?commit=true"

# add few docs
for i in {4001..4500}
do curl -s "http://localhost:48983/solr/mycoll2/update" -X POST \
  -d "[{'id':'$i'}]" -H "Content-type: application/json" > /dev/null
done

sleep 2

# stop/kill leader
docker exec tlogrestart_solr1_1 /bin/bash -c "/opt/solr/bin/solr stop -all"

# add few docs (to new leader)
for i in {4601..4900}
do curl -s "http://localhost:47574/solr/mycoll2/update" -X POST \
  -d "[{'id':'$i'}]" -H "Content-type: application/json" > /dev/null
done

# ?commit?
curl "http://localhost:47574/solr/mycoll2/update?commit=true"

sleep 2
# bring back old leader
docker exec tlogrestart_solr1_1 /bin/bash -c "/opt/solr/bin/solr -h solr1 -c -z zk1:2181 -m 4g"
docker exec -it tlogrestart_solr1_1 /bin/bash -c "tail -f /opt/solr/server/logs/solr.log | grep fullCopy"