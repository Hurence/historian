echo "Setting up InfluxDB..."
curl -X POST -G 'http://influxdb:8086/query' --data-urlencode 'q=CREATE DATABASE solr_metrics'

echo "Setting up Graphana..."
curl -v 'http://grafana:3000/api/datasources' -H 'Content-Type: application/json' --data '{"name":"Apache Solr","type":"influxdb","url":"http://influxdb:8086","access":"proxy","jsonData":{},"secureJsonFields":{},"user":"admin","password":"admin","database":"solr_metrics"}' --user admin:admin


curl 'http://grafana:3000/api/dashboards/import' -H 'Content-Type: application/json' --user admin:admin --data '{"dashboard":{"__inputs":[{"name":"DS_SOLR","label":"Apache Solr","description":"","type":"datasource","pluginId":"influxdb","pluginName":"InfluxDB"}],"__requires":[{"type":"grafana","id":"grafana","name":"Grafana","version":"4.6.3"},{"type":"panel","id":"graph","name":"Graph","version":""},{"type":"datasource","id":"influxdb","name":"InfluxDB","version":"1.0.0"}],"annotations":{"list":[{"builtIn":1,"datasource":"-- Grafana --","enable":true,"hide":true,"iconColor":"rgba(0, 211, 255, 1)","name":"Annotations & Alerts","type":"dashboard"}]},"editable":true,"gnetId":null,"graphTooltip":0,"hideControls":false,"id":null,"links":[],"rows":[{"collapse":false,"height":266,"panels":[{"aliasColors":{},"bars":false,"dashLength":10,"dashes":false,"datasource":"${DS_SOLR}","fill":1,"id":1,"legend":{"avg":false,"current":false,"max":false,"min":false,"show":true,"total":false,"values":false},"lines":true,"linewidth":1,"links":[],"nullPointMode":"null","percentage":false,"pointradius":5,"points":false,"renderer":"flot","seriesOverrides":[],"spaceLength":10,"span":6,"stack":false,"steppedLine":false,"targets":[{"dsType":"influxdb","groupBy":[{"params":["10s"],"type":"time"},{"params":["null"],"type":"fill"}],"measurement":"UPDATE./update.requestTimes","orderByTime":"ASC","policy":"default","query":"SELECT mean(\"one-minute\") FROM \"UPDATE./update.requestTimes\" GROUP BY ;","rawQuery":false,"refId":"A","resultFormat":"time_series","select":[[{"params":["one-minute"],"type":"field"},{"params":[],"type":"mean"},{"params":["/60"],"type":"math"}]],"tags":[]}],"thresholds":[],"timeFrom":"1h","timeShift":null,"title":"Indexing throughput (requests / sec)","tooltip":{"shared":true,"sort":0,"value_type":"individual"},"type":"graph","xaxis":{"buckets":null,"mode":"time","name":null,"show":true,"values":[]},"yaxes":[{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true},{"format":"wps","label":null,"logBase":1,"max":null,"min":null,"show":true}]},{"aliasColors":{},"bars":false,"dashLength":10,"dashes":false,"datasource":"${DS_SOLR}","fill":1,"id":4,"legend":{"avg":false,"current":false,"max":false,"min":false,"show":true,"total":false,"values":false},"lines":true,"linewidth":1,"links":[],"nullPointMode":"null","percentage":false,"pointradius":5,"points":false,"renderer":"flot","seriesOverrides":[],"spaceLength":10,"span":6,"stack":false,"steppedLine":false,"targets":[{"dsType":"influxdb","groupBy":[{"params":["10s"],"type":"time"},{"params":["null"],"type":"fill"}],"measurement":"UPDATE.updateHandler.cumulativeAdds","orderByTime":"ASC","policy":"default","refId":"A","resultFormat":"time_series","select":[[{"params":["one-minute"],"type":"field"},{"params":[],"type":"mean"},{"params":["/60"],"type":"math"}]],"tags":[]}],"thresholds":[],"timeFrom":"1h","timeShift":null,"title":"Indexing throughput (docs/seq)","tooltip":{"shared":true,"sort":0,"value_type":"individual"},"type":"graph","xaxis":{"buckets":null,"mode":"time","name":null,"show":true,"values":[]},"yaxes":[{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true},{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true}]}],"repeat":null,"repeatIteration":null,"repeatRowId":null,"showTitle":false,"title":"Dashboard Row","titleSize":"h6"},{"collapse":false,"height":213,"panels":[{"aliasColors":{},"bars":false,"dashLength":10,"dashes":false,"datasource":"${DS_SOLR}","fill":1,"id":2,"legend":{"avg":false,"current":false,"max":false,"min":false,"show":true,"total":false,"values":false},"lines":true,"linewidth":1,"links":[],"nullPointMode":"null","percentage":false,"pointradius":5,"points":false,"renderer":"flot","seriesOverrides":[],"spaceLength":10,"span":6,"stack":false,"steppedLine":false,"targets":[{"dsType":"influxdb","groupBy":[{"params":["10s"],"type":"time"},{"params":["null"],"type":"fill"}],"measurement":"QUERY./select.requestTimes","orderByTime":"ASC","policy":"default","query":"SELECT mean(\"99-percentile\"), mean(\"95-percentile\")  FROM \"QUERY./select.requestTimes\" WHERE $timeFilter GROUP BY time(10s) fill(0)","rawQuery":true,"refId":"A","resultFormat":"time_series","select":[[{"params":["99-percentile"],"type":"field"},{"params":[],"type":"mean"}]],"tags":[]}],"thresholds":[],"timeFrom":"1h","timeShift":null,"title":"Query Latency","tooltip":{"shared":true,"sort":0,"value_type":"individual"},"type":"graph","xaxis":{"buckets":null,"mode":"time","name":null,"show":true,"values":[]},"yaxes":[{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true},{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true}]},{"aliasColors":{},"bars":false,"dashLength":10,"dashes":false,"datasource":"${DS_SOLR}","fill":1,"id":3,"legend":{"avg":false,"current":false,"max":false,"min":false,"show":true,"total":false,"values":false},"lines":true,"linewidth":1,"links":[],"nullPointMode":"null","percentage":false,"pointradius":5,"points":false,"renderer":"flot","seriesOverrides":[],"spaceLength":10,"span":6,"stack":false,"steppedLine":false,"targets":[{"dsType":"influxdb","groupBy":[{"params":["10s"],"type":"time"},{"params":["null"],"type":"fill"}],"measurement":"QUERY./select.requestTimes","orderByTime":"ASC","policy":"default","refId":"A","resultFormat":"time_series","select":[[{"params":["one-minute"],"type":"field"},{"params":[],"type":"max"}]],"tags":[]}],"thresholds":[],"timeFrom":"1h","timeShift":null,"title":"Query Throughput","tooltip":{"shared":true,"sort":0,"value_type":"individual"},"type":"graph","xaxis":{"buckets":null,"mode":"time","name":null,"show":true,"values":[]},"yaxes":[{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true},{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true}]}],"repeat":null,"repeatIteration":null,"repeatRowId":null,"showTitle":false,"title":"Dashboard Row","titleSize":"h6"}],"schemaVersion":14,"style":"dark","tags":[],"templating":{"list":[]},"time":{"from":"now-6h","to":"now"},"timepicker":{"refresh_intervals":["5s","10s","30s","1m","5m","15m","30m","1h","2h","1d"],"time_options":["5m","15m","1h","6h","12h","24h","2d","7d","30d"]},"timezone":"","title":"Solr dashboard","version":9},"overwrite":true,"inputs":[{"name":"DS_SOLR","type":"datasource","pluginId":"influxdb","value":"Apache Solr"}]}'
