package com.hurence.timeseries.converter;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.DateInfo;
import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.TimeSeriesUtil;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.functions.*;
import com.hurence.timeseries.metric.MetricType;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.query.QueryEvaluator;
import com.hurence.timeseries.query.TypeFunctions;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class is not thread safe !
 */
public class MeasuresToChunkVersionCurrent implements MeasuresToChunk {

    private List<ChronixTransformation> transformations;
    private List<ChronixAggregation> aggregations;
    private List<ChronixAnalysis> analyses;
    private List<ChronixEncoding> encodings;
    private FunctionValueMap functionValueMap;
    private String chunkOrigin;
    private static final String METRIC_STRING = "first;last;min;max;sum;avg;count;dev;trend;outlier;sax:%s,0.01,%s";

    public MeasuresToChunkVersionCurrent(String chunkOrigin) {
        this.chunkOrigin = chunkOrigin;
    }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }

    /**
     * @param name
     * @param measures expected to be f the same year, month, day
     * @param tags
     * @return
     */
    public Chunk buildChunk(String name, TreeSet<? extends Measure> measures, Map<String, String> tags) {
        if (measures == null || measures.isEmpty())
            throw new IllegalArgumentException("points should not be null or empty");
        MetricTimeSeries chunk = buildMetricTimeSeries(name, measures);
        return convertIntoChunk(chunk, tags);
    }

    @Override
    public Chunk buildChunk(String name,
                            TreeSet<? extends Measure> measures) {
        return buildChunk(name, measures, Collections.emptyMap());
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

    private Chunk convertIntoChunk(MetricTimeSeries timeSerie, Map<String, String> tags) {


        Chunk.ChunkBuilder builder = Chunk.builder();
        byte[] compressedPoints = BinaryCompactionUtil.serializeTimeseries(timeSerie);
        builder
                .origin(this.chunkOrigin)
                .tags(tags)
                .end(timeSerie.getEnd())
                .name(timeSerie.getName())
                .start(timeSerie.getStart())
                .value(compressedPoints)
                .version(getVersion());

        computeAndSetAggs(builder, timeSerie);
        DateInfo dateInfo = TimeSeriesUtil.calculDateFields(timeSerie.getStart());
        builder
                .year(dateInfo.year)
                .month(dateInfo.month)
                .day(dateInfo.day)
                .buildId();

        return builder.build();
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    private void computeAndSetAggs(Chunk.ChunkBuilder builder, MetricTimeSeries timeSeries) {
        Integer sax_alphabet_size = Math.max(Math.min(timeSeries.size(), 7), 2);
        Integer sax_string_length = Math.min(timeSeries.size(), 100);
        String metricString = String.format(METRIC_STRING, sax_alphabet_size, sax_string_length);
        String[] metrics = new String[]{"metric{" + metricString + "}"};
        configMetricsCalcul(metrics);


        // set quality stats if needed
        DescriptiveStatistics qualityStats =  new DescriptiveStatistics();
        for( float quality : timeSeries.getQualitiesAsArray()){
            qualityStats.addValue(quality);
        }
        if(qualityStats.getN()>0){
            builder.qualityFirst(timeSeries.getQuality(0));
            builder.qualitySum((float) qualityStats.getSum());
            builder.qualityMin((float) qualityStats.getMin());
            builder.qualityMax((float) qualityStats.getMax());
            builder.qualityAvg((float) qualityStats.getMean());
        }


        functionValueMap.resetValues();
        transformations.forEach(transfo -> transfo.execute(timeSeries, functionValueMap));
        analyses.forEach(analyse -> analyse.execute(timeSeries, functionValueMap));
        aggregations.forEach(aggregation -> aggregation.execute(timeSeries, functionValueMap));
        encodings.forEach(encoding -> encoding.execute(timeSeries, functionValueMap));

        for (int i = 0; i < functionValueMap.sizeOfAggregations(); i++) {
            String name = functionValueMap.getAggregation(i).getQueryName();
            double value = functionValueMap.getAggregationValue(i);
            switch (name) {
                case "first":
                    builder.first(value);
                    break;
                case "last":
                    builder.last(value);
                    break;
                case "min":
                    builder.min(value);
                    break;
                case "max":
                    builder.max(value);
                    break;
                case "sum":
                    builder.sum(value);
                    break;
                case "avg":
                    builder.avg(value);
                    break;
                case "count":
                    builder.count((long) value);
                    break;
                case "dev":
                    builder.stdDev(value);
                    break;
            }
        }
        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
            String name = functionValueMap.getAnalysis(i).getQueryName();
            boolean value = functionValueMap.getAnalysisValue(i);
            switch (name) {
                case "trend":
                    builder.trend(value);
                    break;
                case "outlier":
                    builder.outlier(value);
                    break;
            }
        }
        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
            String name = functionValueMap.getEncoding(i).getQueryName();
            String value = functionValueMap.getEncodingValue(i);
            switch (name) {
                case "sax":
                    builder.sax(value);
                    break;
            }
        }
    }


    private MetricTimeSeries buildMetricTimeSeries(String name, TreeSet<? extends Measure> points) {
        final long start = getStart(points);
        final long end = getEnd(points);
        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(name);
        tsBuilder.start(start);
        tsBuilder.end(end);
        points.forEach(p -> {
            if (p.hasQuality())
                tsBuilder.point(p.getTimestamp(), p.getValue(), p.getQuality());
            else
                tsBuilder.point(p.getTimestamp(), p.getValue());
        });
        return tsBuilder.build();
    }

    private long getEnd(TreeSet<? extends Measure> points) {
        return points.last().getTimestamp();
    }

    private long getStart(TreeSet<? extends Measure> points) {
        return points.first().getTimestamp();
    }
}