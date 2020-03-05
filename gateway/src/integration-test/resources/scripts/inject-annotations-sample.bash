#!/usr/bin/env bash

declare -r ts=$(date +%s%N | cut -b1-13)

echo "injecting one annotation, ts calculated is ${ts}"

curl -X POST -H 'Content-Type: application/json' 'http://solr1:8983/solr/annotation/update/json/docs' --data-binary '
{
  "time": '"${ts}"',
  "text": "a ponctual event at '"${ts}"'",
  "tags": "ponctual",
}'

echo "injecting one ranged annotation"

curl -X POST -H 'Content-Type: application/json' 'http://solr1:8983/solr/annotation/update/json/docs?commit=true' --data-binary '
{
  "time": '"$(("${ts}" - 50000))"',
  "timeEnd": '"${ts}"',
  "text": "a ranged event at '"${ts}"'",
  "tags": ["range", "info"],
}'