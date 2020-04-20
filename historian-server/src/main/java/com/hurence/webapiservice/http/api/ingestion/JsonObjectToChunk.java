package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.converter.common.DoubleList;
import com.hurence.logisland.timeseries.converter.common.LongList;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.Base64;

import static com.hurence.historian.modele.HistorianFields.*;

public class JsonObjectToChunk {

    private static String metricType = "timeseries";

//    public SolrDocument chunkIntoSolrDocument(String metricName, long[] timestamps, double[] values, double[] quality) {
//        MetricTimeSeries chunk = buildMetricTimeSeries(json);
//        return convertIntoSolrDocument(chunk);
//    }
    public SolrInputDocument chunkIntoSolrDocument(JsonObject json) {
        MetricTimeSeries chunk = buildMetricTimeSeries(json);
        return convertIntoSolrInputDocument(chunk);
    }

    private SolrInputDocument convertIntoSolrInputDocument(MetricTimeSeries chunk) {
        final SolrInputDocument doc = new SolrInputDocument();
        checkChunkNotEmpty(chunk);
        doc.addField(NAME, chunk.getName());
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

    private void checkChunkNotEmpty(MetricTimeSeries chunk) {
        if (chunk.isEmpty())
            throw new IllegalArgumentException("chunk is empty !");
        else if(chunk.getName().isEmpty())
            throw new IllegalArgumentException("chunk name is empty !");
        else if(chunk.getValues().isEmpty())
            throw new IllegalArgumentException("chunk values are empty !");

    }


    /**
     *
     * @param json
     *                          <pre>
     *
     *                              {
     *                                  {@value HistorianFields#NAME} : "metric name to add datapoints",
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
        String metricName = json.getString(NAME);
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

        int size = points.size();
        long[] timestamps = new long[size];
        double[] values = new double[size];
        int i = 0;
        for (Object point : points) {
            JsonArray jsonpoint = (JsonArray) point;
            timestamps[i] = jsonpoint.getLong(0);
            values[i] = jsonpoint.getDouble(1);
            i++;
        }
        LongList timestampsList = new LongList(timestamps, size);
        DoubleList valuesList = new DoubleList(values, size);
        tsBuilder.points(timestampsList, valuesList);
    }

    private long getEnd(JsonArray points) {
        JsonArray firstArray = points.getJsonArray(0);
        return firstArray.getLong(0);
    }

    private long getStart(JsonArray points) {
        JsonArray lastArray = points.getJsonArray(points.size()-1);
        return lastArray.getLong(0);
    }
}
