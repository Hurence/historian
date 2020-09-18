#!/usr/bin/env bash


print_usage(){
    cat << EOF

    by Hurence, 09/01/2019
EOF
}

curl_request() {

   curl \-X POST \http://localhost:8080/historian-server/ingestion/csv \
  -F 'my_csv_file=@/Users/wichroff/Documents/Git/Hurence/historian/historian-resources/api-release-historian/metrics_test.csv' \
  -F 'my_csv_file2=@/Users/wichroff/Documents/Git/Hurence/historian/historian-resources/api-release-historian/metrics_test.csv' \
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
>result_command_line_server.txt

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
}'> result_command_line_default.txt


}

diff_between_files() {
diff -s -b result_command_line_server.txt result_command_line_default.txt
rm result_command_line_server.txt
rm result_command_line_default.txt
}

main() {
    curl_request
    diff_between_files
}

main "$@"