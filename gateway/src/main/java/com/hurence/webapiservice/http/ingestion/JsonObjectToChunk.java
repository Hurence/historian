package com.hurence.webapiservice.http.ingestion;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionUtil;
import com.hurence.webapiservice.historian.HistorianFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.util.Base64;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static com.hurence.webapiservice.historian.HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD;

public class JsonObjectToChunk {

    private static String metricType = "timeseries";

//    public SolrDocument chunkIntoSolrDocument(String metricName, long[] timestamps, double[] values, double[] quality) {
//        MetricTimeSeries chunk = buildMetricTimeSeries(json);
//        return convertIntoSolrDocument(chunk);
//    }
    public SolrDocument chunkIntoSolrDocument(JsonObject json) {
        MetricTimeSeries chunk = buildMetricTimeSeries(json);
        return convertIntoSolrDocument(chunk);
    }

    private SolrDocument convertIntoSolrDocument(MetricTimeSeries chunk) {
        final SolrDocument doc = new SolrDocument();
        doc.addField(RESPONSE_METRIC_NAME_FIELD, chunk.getName());
        doc.addField(RESPONSE_CHUNK_START_FIELD, chunk.getStart());
        doc.addField(RESPONSE_CHUNK_END_FIELD, chunk.getEnd());
        doc.addField(RESPONSE_CHUNK_SIZE_FIELD, chunk.getValues().size());
        doc.addField(RESPONSE_CHUNK_WINDOW_MS_FIELD,  chunk.getEnd() - chunk.getStart());
        chunk.attributes().keySet().forEach(key -> {
            doc.addField(key, chunk.attribute(key));
        });
        byte[] compressedPoints = BinaryCompactionUtil.serializeTimeseries(chunk);
        doc.addField(RESPONSE_CHUNK_VALUE_FIELD, Base64.getEncoder().encodeToString(compressedPoints));
        doc.addField(RESPONSE_CHUNK_SIZE_BYTES_FIELD, compressedPoints.length);
        return doc;
    }
    public SolrInputDocument buildSolrDocument(String id) {

        final SolrInputDocument doc = new SolrInputDocument();//TODO last remaining fields
        doc.addField(RESPONSE_CHUNK_ID_FIELD, id);
//        doc.addField(RESPONSE_CHUNK_START_FIELD, this.start);
//        doc.addField(RESPONSE_CHUNK_SIZE_FIELD, this.points.size());
//        doc.addField(RESPONSE_CHUNK_END_FIELD, this.end);
//        doc.addField(RESPONSE_CHUNK_SAX_FIELD, this.sax);
//        doc.addField(RESPONSE_CHUNK_VALUE_FIELD, this.compressedPoints);
//        doc.addField(RESPONSE_CHUNK_VALUE_FIELD, Base64.getEncoder().encodeToString(this.compressedPoints));
//        doc.addField(RESPONSE_CHUNK_AVG_FIELD, this.avg);
//        doc.addField(RESPONSE_CHUNK_MIN_FIELD, this.min);
//        doc.addField(RESPONSE_CHUNK_WINDOW_MS_FIELD, 11855);
//        doc.addField(RESPONSE_METRIC_NAME_FIELD, this.name);
//        doc.addField(RESPONSE_CHUNK_TREND_FIELD, this.trend);
//        doc.addField(RESPONSE_CHUNK_MAX_FIELD, this.max);
//        doc.addField(RESPONSE_CHUNK_SIZE_BYTES_FIELD, this.compressedPoints.length);
//        doc.addField(RESPONSE_CHUNK_SUM_FIELD, this.sum);
//        doc.addField(RESPONSE_TAG_NAME_FIELD, this.tags);
//        doc.addField(RESPONSE_CHUNK_FIRST_VALUE_FIELD, this.firstValue);
        return doc;
    }

    /**
     *
     * @param json
     *                          <pre>
     *
     *                              {
     *                                  {@value HistorianFields#METRIC_NAME_REQUEST_FIELD} : "metric name to add datapoints",
     *                                  {@value HistorianFields#POINTS_REQUEST_FIELD } : [
     *                                      [timestamp, value, quality]
     *                                      ...
     *                                      [timestamp, value, quality]
     *                                  ]
     *                              }
     *
     *                          </pre>
     * @return
     */
    private MetricTimeSeries buildMetricTimeSeries(JsonObject json) {
        String metricName = json.getString(METRIC_NAME_REQUEST_FIELD);
        JsonArray points = json.getJsonArray(POINTS_REQUEST_FIELD);

        final long start = getStart(points);
        final long end  = getEnd(points);

        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(metricName, metricType);
        tsBuilder.start(start);
        tsBuilder.end(end);
        // If want to add nay attributes
        // tsBuilder.attribute(tagKey, tagValue);
        addPoints(tsBuilder, points);
        return tsBuilder.build();
    }

    private void addPoints(MetricTimeSeries.Builder tsBuilder, JsonArray points) {
//        records.forEach(record -> {
//            if (record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null) {
//                final long timestamp = record.getTime().getTime();
//                final double value = record.getField(FieldDictionary.RECORD_VALUE).asDouble();
//                tsBuilder.point(timestamp, value);
//            }
//        });
        //TODO
    }

    private long getEnd(JsonArray points) {
        return 0;//TODO
    }

    private long getStart(JsonArray points) {
        return 0;//TODO
    }
}
