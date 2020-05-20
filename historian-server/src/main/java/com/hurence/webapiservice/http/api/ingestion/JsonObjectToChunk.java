package com.hurence.webapiservice.http.api.ingestion;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.HistorianFields;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.logisland.timeseries.DateInfo;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.TimeSeriesUtil;
import com.hurence.logisland.timeseries.converter.common.DoubleList;
import com.hurence.logisland.timeseries.converter.common.LongList;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionUtil;
import com.hurence.logisland.timeseries.functions.*;
import com.hurence.logisland.timeseries.metric.MetricType;
import com.hurence.logisland.timeseries.query.QueryEvaluator;
import com.hurence.logisland.timeseries.query.TypeFunctions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static com.hurence.historian.modele.HistorianFields.*;

/**
 * This class is not thread safe !
 */
public class JsonObjectToChunk {

    private static String metricType = "timeseries";

    private List<ChronixTransformation> transformations;
    private List<ChronixAggregation> aggregations;
    private List<ChronixAnalysis> analyses;
    private List<ChronixEncoding> encodings;
    private FunctionValueMap functionValueMap;
    private String chunkOrigin;

    public JsonObjectToChunk(String chunkOrigin) {
        this.chunkOrigin = chunkOrigin;
    }


    private void configMetricsCalcul(String[] metrics) {
        // init metric functions
        TypeFunctions functions = QueryEvaluator.extractFunctions(metrics);
        transformations = functions.getTypeFunctions(new MetricType()).getTransformations();
        aggregations = functions.getTypeFunctions(new MetricType()).getAggregations();
        analyses = functions.getTypeFunctions(new MetricType()).getAnalyses();
        encodings = functions.getTypeFunctions(new MetricType()).getEncodings();
        functionValueMap = new FunctionValueMap(aggregations.size(), analyses.size(), transformations.size(), encodings.size());
    }

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
        doc.addField(RESPONSE_CHUNK_COUNT_FIELD, chunk.getValues().size());
        chunk.attributes().keySet().forEach(key -> {
            doc.addField(key, chunk.attribute(key));
        });
        byte[] compressedPoints = BinaryCompactionUtil.serializeTimeseries(chunk);
        doc.addField(RESPONSE_CHUNK_VALUE_FIELD, Base64.getEncoder().encodeToString(compressedPoints));
        doc.addField(RESPONSE_CHUNK_SIZE_BYTES_FIELD, compressedPoints.length);
        computeAndSetMetrics(doc, chunk);
        DateInfo dateInfo = TimeSeriesUtil.calculDateFields(chunk.getStart());
        doc.addField(CHUNK_YEAR, dateInfo.year);
        doc.addField(CHUNK_DAY, dateInfo.day);
        doc.addField(CHUNK_MONTH, dateInfo.month);
        doc.addField(CHUNK_ORIGIN, this.chunkOrigin);
        doc.setField(RESPONSE_CHUNK_ID_FIELD, calulateHash(doc));
        return doc;
    }

    /**
     * calculate sha256 of value, start and name
     *
     * @param doc
     * @return
     */
    private String calulateHash(SolrInputDocument doc) {
        String toHash = doc.getField(TimeSeriesRecord.CHUNK_VALUE).toString() +
                doc.getField(TimeSeriesRecord.METRIC_NAME).toString() +
                doc.getField(TimeSeriesRecord.CHUNK_START).toString() +
                doc.getField(TimeSeriesRecord.CHUNK_ORIGIN).toString();

        String sha256hex = Hashing.sha256()
                .hashString(toHash, StandardCharsets.UTF_8)
                .toString();

        return sha256hex;
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    private void computeAndSetMetrics(SolrInputDocument doc, MetricTimeSeries timeSeries) {
        Integer sax_alphabet_size = Math.max(Math.min(timeSeries.size(), 7), 2);
        Integer sax_string_length = Math.min(timeSeries.size(), 100);
        String metricString = String.format("first;min;max;sum;avg;trend;outlier;sax:%s,0.01,%s", sax_alphabet_size, sax_string_length);
        String[] metrics = new String[]{"metric{" + metricString + "}"};
        configMetricsCalcul(metrics);

        functionValueMap.resetValues();
        transformations.forEach(transfo -> transfo.execute(timeSeries, functionValueMap));
        analyses.forEach(analyse -> analyse.execute(timeSeries, functionValueMap));
        aggregations.forEach(aggregation -> aggregation.execute(timeSeries, functionValueMap));
        encodings.forEach(encoding -> encoding.execute(timeSeries, functionValueMap));

        for (int i = 0; i < functionValueMap.sizeOfAggregations(); i++) {
            String name = functionValueMap.getAggregation(i).getQueryName();
            double value = functionValueMap.getAggregationValue(i);
            doc.setField("chunk_" + name, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
            String name = functionValueMap.getAnalysis(i).getQueryName();
            boolean value = functionValueMap.getAnalysisValue(i);
            doc.setField("chunk_" + name, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
            String name = functionValueMap.getEncoding(i).getQueryName();
            String value = functionValueMap.getEncodingValue(i);
            doc.setField("chunk_" + name, value);
        }
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
        JsonObject tags = json.getJsonObject(TAGS, new JsonObject());

        final long start = getStart(points);
        final long end  = getEnd(points);

        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(metricName, metricType);
        tsBuilder.start(start);
        tsBuilder.end(end);
        // If want to add nay attributes
        //tsBuilder.attribute(tagKey, tagValue);
        JsonArray allTagsAsArray = new JsonArray();
        tags.getMap().forEach((key, value) -> {
            allTagsAsArray.add(key);
            tsBuilder.attribute(key,value.toString());
        });
        addPoints(tsBuilder, points);
        return tsBuilder.build();
    }

    private void addPoints(MetricTimeSeries.Builder tsBuilder, JsonArray points) {

        int size = points.size();
        long[] timestamps = new long[size];
        double[] values = new double[size];
        int i = 0;
        for (Object point : points) {
            JsonArray jsonPoint = (JsonArray) point;
            timestamps[i] = jsonPoint.getLong(0);
            values[i] = jsonPoint.getDouble(1);
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
