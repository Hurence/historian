curl "http://localhost:8983/solr/mycoll2/update?commit=true"
for i in {1..1000000}
  do echo $i; curl "http://localhost:8983/solr/mycoll2/select?q=id:$i&omitHeader=true" 
done
