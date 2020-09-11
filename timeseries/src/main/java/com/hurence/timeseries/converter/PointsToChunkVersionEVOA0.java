package com.hurence.timeseries.converter;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.DateInfo;
import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.TimeSeriesUtil;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.functions.*;
import com.hurence.timeseries.metric.MetricType;
import com.hurence.timeseries.modele.chunk.Chunk;
import com.hurence.timeseries.modele.chunk.ChunkVersionEVOA0;
import com.hurence.timeseries.modele.chunk.ChunkVersionEVOA0Impl;
import com.hurence.timeseries.modele.points.Point;
import com.hurence.timeseries.query.QueryEvaluator;
import com.hurence.timeseries.query.TypeFunctions;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class is not thread safe !
 */
public class PointsToChunkVersionEVOA0 implements PointsToChunk {

    private List<ChronixTransformation> transformations;
    private List<ChronixAggregation> aggregations;
    private List<ChronixAnalysis> analyses;
    private List<ChronixEncoding> encodings;
    private FunctionValueMap functionValueMap;
    private static final String METRIC_STRING = "first;min;max;sum;avg;count;trend;sax:%s,0.01,%s";

    public PointsToChunkVersionEVOA0() { }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.EVOA0;
    }

    public ChunkVersionEVOA0 buildChunk(String name, TreeSet<? extends Point> points, Map<String, String> tags) {
        if (points == null || points.isEmpty())
            throw new IllegalArgumentException("points should not be null or empty");
        MetricTimeSeries chunk = buildMetricTimeSeries(name, points);
        return convertIntoChunk(chunk, tags);
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

    private ChunkVersionEVOA0 convertIntoChunk(MetricTimeSeries timeSerie, Map<String, String> tags) {
        ChunkVersionEVOA0Impl.Builder builder = new ChunkVersionEVOA0Impl.Builder();
        byte[] compressedPoints = BinaryCompactionUtil.serializeTimeseries(timeSerie);
        builder
                .setTags(tags)
                .setEnd(timeSerie.getEnd())
                .setName(timeSerie.getName())
                .setStart(timeSerie.getStart())
                .setValueBinaries(compressedPoints)
                .setVersion(getVersion());
        computeAndSetAggs(builder, timeSerie);
        DateInfo dateInfo = TimeSeriesUtil.calculDateFields(timeSerie.getStart());
        builder
                .setYear(dateInfo.year)
                .setMonth(dateInfo.month)
                .setDay(dateInfo.day);
        return builder.build();
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    private void computeAndSetAggs(ChunkVersionEVOA0Impl.Builder builder, MetricTimeSeries timeSeries) {
        Integer sax_alphabet_size = Math.max(Math.min(timeSeries.size(), 7), 2);
        Integer sax_string_length = Math.min(timeSeries.size(), 100);
        String metricString = String.format(METRIC_STRING, sax_alphabet_size, sax_string_length);
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
            switch (name) {
                case "first" :
                    builder.setFirst(value);
                    break;
                case "min" :
                    builder.setMin(value);
                    break;
                case "max" :
                    builder.setMax(value);
                    break;
                case "sum" :
                    builder.setSum(value);
                    break;
                case "avg" :
                    builder.setAvg(value);
                    break;
                case "count" :
                    builder.setCount((long)value);
                    break;
            }
        }
        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
            String name = functionValueMap.getAnalysis(i).getQueryName();
            boolean value = functionValueMap.getAnalysisValue(i);
            switch (name) {
                case "trend" :
                    builder.setTrend(value);
                    break;
            }
        }
        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
            String name = functionValueMap.getEncoding(i).getQueryName();
            String value = functionValueMap.getEncodingValue(i);
            switch (name) {
                case "sax" :
                    builder.setSax(value);
                    break;
            }
        }
    }


    private MetricTimeSeries buildMetricTimeSeries(String name, TreeSet<? extends Point> points) {
        final long start = getStart(points);
        final long end  = getEnd(points);
        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(name);
        tsBuilder.start(start);
        tsBuilder.end(end);
        points.forEach(p -> {
            tsBuilder.point(p.getTimestamp(), p.getValue());
        });
        return tsBuilder.build();
    }

    private long getEnd(TreeSet<? extends Point> points) {
        return points.last().getTimestamp();
    }

    private long getStart(TreeSet<? extends Point> points) {
        return points.first().getTimestamp();
    }
}
