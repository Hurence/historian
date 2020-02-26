#!/usr/bin/env bash

echo "injecting chunks, ts calculated is ${ts}"

declare -r ts=$(date +%s%N | cut -b1-13)

curl -X POST -H 'Content-Type: application/json' 'http://solr1:8983/solr/historian/update?commit=true' --data-binary '
[
  {
    "name" : "temp_a",
    "chunk_first": 1,
    "chunk_avg": 1,
    "chunk_end": '"${ts}"',
    "chunk_max": 1,
    "chunk_min": 1,
    "chunk_sum": 40,
    "chunk_sax": "aaa",
    "chunk_size": "40",
    "chunk_start": '"$(("${ts}" - 50000))"',
    "chunk_value": "H4sIAAAAAAAAAOPi1GQAgw/2XCwKjAYMXEyDELMIAF0mwAAAJ4X9oq0AAAA="
  },
  {
    "name" : "temp_b",
    "chunk_first": 1,
    "chunk_avg": 1,
    "chunk_end": '"${ts}"',
    "chunk_max": 1,
    "chunk_min": 1,
    "chunk_sum": 40,
    "chunk_sax": "aaa",
    "chunk_size": "10",
    "chunk_start": '"$(("${ts}" - 50000))"',
    "chunk_value": "H4sIAAAAAAAAAOPi1GQAgw/2XCwKjAYMXEyDELMIAF0mwAAAJ4X9oq0AAAA="
  }
]'