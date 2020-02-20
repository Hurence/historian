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
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.util.string.BinaryEncodingUtils;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Timeseries holder record
 */
public class TimeSeriesRecord extends StandardRecord {

    public static final String CHUNK_ORIGIN = "chunk_origin";
    private MetricTimeSeries timeSeries;

    private static final BinaryCompactionConverter converter = new BinaryCompactionConverter.Builder().build();


    public static final String RECORD_CHUNK_COMPRESSED_POINTS = "record_chunk_compressed_points";
    public static final String RECORD_CHUNK_UNCOMPRESSED_POINTS = "record_chunk_uncompressed_points";
    public static final String RECORD_CHUNK_SAX_POINTS = "record_chunk_sax_points";
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

    public TimeSeriesRecord(String type, String id, String name, String chunkValue, long chunkStart, long chunkEnd) {
        super(type);

        setId(id);
        setStringField(METRIC_NAME, name);
        setStringField(CHUNK_VALUE, chunkValue);
        setField(CHUNK_START, FieldType.LONG, chunkStart);
        setField(CHUNK_END, FieldType.LONG, chunkEnd);


        try {
            byte[] chunkBytes = BinaryEncodingUtils.decode(chunkValue);
            Stream<Point> pointStream = converter.unCompressPoints(chunkBytes, chunkStart, chunkEnd).stream();

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

}
