#!/usr/bin/env bash

################################
# GLOBAL VARIABLES
################################

# color setup
NOCOLOR='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
LIGHTGRAY='\033[0;37m'
DARKGRAY='\033[1;30m'
LIGHTRED='\033[1;31m'
LIGHTGREEN='\033[1;32m'
YELLOW='\033[1;33m'
LIGHTBLUE='\033[1;34m'
LIGHTPURPLE='\033[1;35m'
LIGHTCYAN='\033[1;36m'
WHITE='\033[1;37m'

#delete specified collection
#param1 the solr url (ie localhost:8983/solr)
#param2 the name of the collection to delete
delete_collection() {
  local solr_host="$1"
  local collection_name="$2"
  echo -e "${RED}will delete collection ${collection_name} on ${solr_host} ${NOCOLOR}"
  curl "http://${solr_host}/admin/collections?action=DELETE&name=${collection_name}"
}

#crate specified collection
#param1 the solr url (ie localhost:8983/solr)
#param2 the name of the collection to delete
create_collection() {
  local solr_host="$1"
  local collection_name="$2"
  echo -e "${GREEN}will create collection ${collection_name} on ${solr_host} with ${NUM_SHARDS} shard and ${REPLICATION_FACTOR} replicas ${NOCOLOR}"
  curl "http://${solr_host}/admin/collections?action=CREATE&name=${collection_name}&numShards=${NUM_SHARDS}&replicationFactor=${REPLICATION_FACTOR}"
  echo "waiting 5' for changes propagation"
  sleep 5
  curl -X POST -H 'Content-type:application/json' -d "{\"set-user-property\": {\"update.autoCreateFields\":\"false\"}}}" "http://${solr_host}/${collection_name}/config"
  sleep 1
}

#append specfieid string to variable
#param1 variable name to append string
#param2 the string to append
append_string_to_variable() {
  local variable_name_to_modify="$1"
  local -n variable_value="${variable_name_to_modify}"
  local string_to_append="$2"

  export "${variable_name_to_modify}=${variable_value}${string_to_append}"
}

#append a field add update to specified variable
#param1 variable to append that
#param2 field name to add
#param3 field type to add
add_field_name_type_to_variable() {
  local variable_name_to_append="$1"
  local field_name="$2"
  local field_type="$3"

  local string_to_append=", \"add-field\": { \"name\":\"$field_name\", \"type\":\"$field_type\" }"
  append_string_to_variable "${variable_name_to_append}" "${string_to_append}"
}

#append a field add update to specified variable
#param1 variable to append that
#param2 field name to add
#param3 field type to add
add_field_to_variable() {
  local variable_name_to_append="$1"
  local field_name="$2"
  local field_type="$3"

  local string_to_append=", \"add-field\": { \"name\":\"$field_name\", \"type\":\"$field_type\", \"indexed\":true, \"stored\":true, \"multiValued\":false }"
  append_string_to_variable "${variable_name_to_append}" "${string_to_append}"
}

#append a field add update to specified variable
#param1 variable to append that
#param2 field name to add
add_dynamic_field_to_variable() {
  local variable_name_to_append="$1"
  local field_name="$2"

  local string_to_append=", \"add-dynamic-field\": { \"name\":\"field_name\", \"type\":\"string\", \"indexed\":true, \"stored\":true, \"multiValued\":false  }"
  append_string_to_variable "${variable_name_to_append}" "${string_to_append}"
}

#append a field add update to specified variable
#param1 variable to append that
#param2 field name to add
#param3 field type to add
add_field_multivalued_to_variable() {
  local variable_name_to_append="$1"
  local field_name="$2"
  local field_type="$3"

  local string_to_append=", \"add-field\": { \"name\":\"$field_name\", \"type\":\"$field_type\", \"indexed\":true, \"stored\":true, \"multiValued\":true }"
  append_string_to_variable "${variable_name_to_append}" "${string_to_append}"
}

#append a field add update to specified variable
#param1 variable to append that
#param2 field name to add
#param3 field type to add
add_field_not_indexed_to_variable() {
  local variable_name_to_append="$1"
  local field_name="$2"
  local field_type="$3"

  local string_to_append=", \"add-field\": { \"name\":\"$field_name\", \"type\":\"$field_type\", \"indexed\":false, \"multiValued\":false }"
  append_string_to_variable "${variable_name_to_append}" "${string_to_append}"
}

#delete specified collection
#param1 the solr url (ie localhost:8983/solr)
#param2 the name of the collection
#param2 the name of the field to delete
delete_field() {
  local solr_host="$1"
  local collection_name="$2"
  local field_name="$3"
  curl -X POST -H 'Content-type:application/json' -d "{ \"delete-field\":{\"name\":\"${field_name}\"}}" "http://${solr_host}/${collection_name}/schema"
}