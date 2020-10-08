#!/usr/bin/env bash


print_usage(){
    cat << EOF

    by Hurence, 09/01/2019


EOF
}

parse_args() {
echo "This script will delete a collection"
echo -e "\n"
echo "Path to the solr cluster ?"
read solr_cluster_path               #variable stocké dans $solr_cluster_path
echo -e "\n"
echo "Name of the collection ?"
read chunk_collection_name           #variable stocké dans $chunk_collection_name

curl "http://${solr_cluster_path}/admin/collections?action=DELETE&name=${chunk_collection_name}"

}

main() {
    parse_args "$@"
}

main "$@"