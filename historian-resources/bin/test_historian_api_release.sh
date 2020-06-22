#!/usr/bin/env bash


print_usage(){
    cat << EOF
    create-historian-collection.sh [options]

    by Hurence, 09/01/2019

    The script creates a collection for historian solr

EOF
}

parse_args() {

bash install.sh

   curl -X POST \
  http://localhost:8080/historian-server/ingestion/csv \
  -F 'my_csv_file=@/Users/wichroff/hdh_workspace/historian-1.3.4-SNAPSHOT/TEST.csv' \
  -F 'my_csv_file2=@/Users/wichroff/hdh_workspace/historian-1.3.4-SNAPSHOT/TEST.csv' \
  -F mapping.name=metric_name_2 \
  -F mapping.value=value_2 \
  -F mapping.timestamp=timestamp \
  -F mapping.quality=quality \
  -F mapping.tags=sensor \
  -F mapping.tags=code_install \
  -F timestamp_unit=MILLISECONDS_EPOCH \
  -F group_by=name \
  -F group_by=tags.sensor \
  -F format_date=yyyy-D-m HH:mm:ss.SSS \
  -F timezone_date=UTC \
>result_command_line.txt

echo '{
  "tags" : [ "sensor", "code_install" ],
  "grouped_by" : [ "name", "sensor" ],
  "report" : [ {
    "name" : "  metric_1",
    "sensor" : "sensor_1",
    "number_of_points_injected" : 4,
    "number_of_point_failed" : 0,
    "number_of_chunk_created" : 2
  }, {
    "name" : "  metric_1",
    "sensor" : "sensor_2",
    "number_of_points_injected" : 2,
    "number_of_point_failed" : 0,
    "number_of_chunk_created" : 2
  }, {
    "name" : "  metric_2",
    "sensor" : "sensor_2",
    "number_of_points_injected" : 2,
    "number_of_point_failed" : 0,
    "number_of_chunk_created" : 2
  } ]
}'> result_command_line1.txt

diff -s -b result_command_line.txt result_command_line1.txt
rm result_command_line.txt
rm result_command_line1.txt
}


main() {
    parse_args "$@"
}

main "$@"