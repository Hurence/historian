/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.record;

import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverterOfRecord;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionUtil;
import com.hurence.logisland.timeseries.functions.*;
import com.hurence.logisland.timeseries.metric.MetricType;
import com.hurence.logisland.timeseries.query.QueryEvaluator;
import com.hurence.logisland.timeseries.query.TypeFunctions;
import com.hurence.logisland.util.string.BinaryEncodingUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Timeseries holder record
 */
public class TimeSeriesRecord extends StandardRecord {


    private static Logger logger = LoggerFactory.getLogger(StandardRecord.class);

    public static final String CHUNK_ORIGIN = "chunk_origin";
    private MetricTimeSeries timeSeries;

    public static final String RECORD_CHUNK_COMPRESSED_POINTS = "record_chunk_compressed_points";
    public static final String RECORD_CHUNK_UNCOMPRESSED_POINTS = "record_chunk_uncompressed_points";
    public static final String RECORD_CHUNK_SAX_POINTS = "record_chunk_sax_points";

    public static final String CHUNK_ID = "id";
    public static final String CHUNK_COMPACTION_RUNNING = "compactions_running";
    public static final String CHUNK_START = "chunk_start";
    public static final String CHUNK_END = "chunk_end";
    public static final String CHUNK_META = "chunk_attribute";
    public static final String CHUNK_MAX = "chunk_max";
    public static final String CHUNK_COUNT = "chunk_count";
    public static final String CHUNK_MIN = "chunk_min";
    public static final String CHUNK_FIRST_VALUE = "chunk_first";
    public static final String CHUNK_AVG = "chunk_avg";
    public static final String CHUNK_SAX = "chunk_sax";
    public static final String CHUNK_TREND = "chunk_trend";
    public static final String CHUNK_OUTLIER = "chunk_outlier";
    public static final String CHUNK_SIZE = "chunk_size";
    public static final String CHUNK_VALUE = "chunk_value";
    public static final String CHUNK_SIZE_BYTES = "chunk_size_bytes";
    public static final String METRIC_NAME = "name";
    public static final String CODE_INSTALL = "code_install";
    public static final String CHUNK_DAY = "chunk_day";
    public static final String CHUNK_MONTH = "chunk_month";
    public static final String CHUNK_YEAR = "chunk_year";
    public static final String CHUNK_WEEK = "chunk_week";
    public static final String SENSOR = "sensor";
    public static final String CHUNK_SUM = "chunk_sum";
    public static final String CHUNK_WINDOW_MS = "chunk_window_ms";
    public static final String RECORD_TIMESERIE_POINT_TIMESTAMP = "record_timeserie_time";
    public static final String RECORD_TIMESERIE_POINT_VALUE = "record_timeserie_value";

    public static final String CHUNK_ORIGIN_COMPACTOR = "compactor";
    public static final String CHUNK_ORIGIN_LOGISLAND = "logisland";
    public static final String CHUNK_ORIGIN_LOADER = "loader";


    public TimeSeriesRecord(MetricTimeSeries timeSeries) {
        super(timeSeries.getType());
        this.timeSeries = timeSeries;

        setStringField(METRIC_NAME, timeSeries.getName());
        setField(CHUNK_START, FieldType.LONG, timeSeries.getStart());
        setField(CHUNK_END, FieldType.LONG, timeSeries.getEnd());
        setField(CHUNK_SIZE, FieldType.INT, timeSeries.getValues().size());
        setField(CHUNK_WINDOW_MS, FieldType.LONG, timeSeries.getEnd() - timeSeries.getStart());
        timeSeries.attributes().keySet().forEach(key -> {
            setStringField(key, String.valueOf(timeSeries.attribute(key)));
        });
    }

    public TimeSeriesRecord(String type, String name, String chunkValue, long chunkStart, long chunkEnd) {
        super(type);

        setStringField(METRIC_NAME, name);
        setStringField(CHUNK_VALUE, chunkValue);
        setField(CHUNK_START, FieldType.LONG, chunkStart);
        setField(CHUNK_END, FieldType.LONG, chunkEnd);

        try {
            Stream<Point> pointStream = getPointStream(chunkValue, chunkStart, chunkEnd).stream();

            MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(name, type)
                    .start(chunkStart)
                    .end(chunkEnd);
            //     .attributes(attributes);
            pointStream.forEach(point -> builder.point(point.getTimestamp(), point.getValue()));
            timeSeries = builder.build();

            setField(CHUNK_SIZE, FieldType.INT, timeSeries.getValues().size());
            setField(CHUNK_WINDOW_MS, FieldType.LONG, timeSeries.getEnd() - timeSeries.getStart());
        } catch (Exception ex) {
            // do nothing ?
        }
    }

    public static List<Point> getPointStream(String chunkValue, long chunkStart, long chunkEnd) throws IOException {
        byte[] chunkBytes = BinaryEncodingUtils.decode(chunkValue);
        return BinaryCompactionUtil.unCompressPoints(chunkBytes, chunkStart, chunkEnd);
    }


    public MetricTimeSeries getTimeSeries() {
        return timeSeries;
    }


    /**
     * get the chunk size
     *
     * @return the number of points or 0 if not initialized
     */
    public int getChunkSize() {
        if (timeSeries != null)
            return timeSeries.size();
        else
            return 0;
    }

    public String getMetricName() {
        if (hasField(METRIC_NAME))
            return getField(METRIC_NAME).asString();
        else
            return "";
    }

    public long getStartChunk() {
        if (hasField(CHUNK_START))
            return getField(CHUNK_START).asLong();
        else
            return 0;
    }

    public long getEndChunk() {
        if (hasField(CHUNK_END))
            return getField(CHUNK_END).asLong();
        else
            return 0;
    }

    public String getChunkValue() {
        if (hasField(CHUNK_VALUE))
            return getField(CHUNK_VALUE).asString();
        else
            return "";
    }

    public double getFirstValue() {
        if (hasField(CHUNK_FIRST_VALUE))
            return getField(CHUNK_FIRST_VALUE).asDouble();
        else
            return 0;
    }

    /**
     *
     * @return uncompressed points lazyly if not yet done
     */
    public List<Point> getPoints() {
        if (hasField(RECORD_CHUNK_UNCOMPRESSED_POINTS))
            return (List<Point>) getField(RECORD_CHUNK_UNCOMPRESSED_POINTS).getRawValue();
        else {
            List<Point> points = null;
            try {
                points = getPointStream(getChunkValue(), getStartChunk(), getEndChunk());
            } catch (IOException e) {
                logger.error("chile uncompressing data points of chunk", e);
                return Collections.emptyList();
            }
            setField(RECORD_CHUNK_UNCOMPRESSED_POINTS, FieldType.ARRAY, points);
            return points;
        }
    }

    public void computeAndSetChunkValueAndChunkSizeBytes() {
        try{
            byte[] bytes = BinaryCompactionUtil.serializeTimeseries(getTimeSeries(), BinaryCompactionUtil.DEFAULT_DDC_THRESHOLD);
            String chunkValueBase64 = BinaryEncodingUtils.encode(bytes);
            this.setStringField(TimeSeriesRecord.CHUNK_VALUE, chunkValueBase64);
            this.setIntField(TimeSeriesRecord.CHUNK_SIZE_BYTES, bytes.length);
        } catch (Exception ex) {
            logger.error(
                    "Unable to convert chunk_vlaue to base64 : {}",
                    new Object[]{ex.getMessage()});
        }
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    public void computeAndSetMetrics(String[] metric) {
        // init metric functions
        TypeFunctions functions = QueryEvaluator.extractFunctions(metric);
        final List<ChronixTransformation> transformations = functions.getTypeFunctions(new MetricType()).getTransformations();
        final List<ChronixAggregation> aggregations = functions.getTypeFunctions(new MetricType()).getAggregations();
        final List<ChronixAnalysis> analyses = functions.getTypeFunctions(new MetricType()).getAnalyses();
        final List<ChronixEncoding> encodings = functions.getTypeFunctions(new MetricType()).getEncodings();
        FunctionValueMap functionValueMap = new FunctionValueMap(aggregations.size(), analyses.size(), transformations.size(), encodings.size());

        MetricTimeSeries timeSeries = this.getTimeSeries();
        functionValueMap.resetValues();

        transformations.forEach(transfo -> transfo.execute(timeSeries, functionValueMap));
        analyses.forEach(analyse -> analyse.execute(timeSeries, functionValueMap));
        aggregations.forEach(aggregation -> aggregation.execute(timeSeries, functionValueMap));
        encodings.forEach(encoding -> encoding.execute(timeSeries, functionValueMap));

        for (int i = 0; i < functionValueMap.sizeOfAggregations(); i++) {
            String name = functionValueMap.getAggregation(i).getQueryName();
            double value = functionValueMap.getAggregationValue(i);
            this.setField("chunk_" + name, FieldType.DOUBLE, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
            String name = functionValueMap.getAnalysis(i).getQueryName();
            boolean value = functionValueMap.getAnalysisValue(i);
            this.setField("chunk_" + name, FieldType.BOOLEAN, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
            String name = functionValueMap.getEncoding(i).getQueryName();
            String value = functionValueMap.getEncodingValue(i);
            this.setField("chunk_" + name, FieldType.STRING, value);
        }
    }
}
