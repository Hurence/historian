/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.record;

import com.hurence.logisland.timeseries.MetricTimeSeries;

/**
 * Timeseries holder record
 */
public class TimeSeriesRecord extends StandardRecord {

    public static final String CHUNK_ORIGIN = "chunk_origin";
    private MetricTimeSeries timeSeries;

    public static final String RECORD_CHUNK_COMPRESSED_POINTS = "record_chunk_compressed_points";
    public static final String RECORD_CHUNK_UNCOMPRESSED_POINTS = "record_chunk_uncompressed_points";
    public static final String RECORD_CHUNK_SAX_POINTS = "record_chunk_sax_points";
    public static final String CHUNK_START = "chunk_start";
    public static final String CHUNK_END = "chunk_end";
    public static final String CHUNK_META = "chunk_attribute";
    public static final String CHUNK_MAX = "chunk_max";
    public static final String CHUNK_MIN = "chunk_min";
    public static final String CHUNK_FIRST_VALUE = "chunk_first";
    public static final String CHUNK_AVG = "chunk_avg";
    public static final String CHUNK_SAX = "chunk_sax";
    public static final String CHUNK_TREND = "chunk_trend";
    public static final String CHUNK_OUTLIER = "chunk_outlier";
    public static final String CHUNK_SIZE = "chunk_size";
    public static final String CHUNK_VALUE = "chunk_value";
    public static final String CHUNK_SIZE_BYTES = "chunk_size_bytes";
    public static final String CHUNK_SUM = "chunk_sum";
    public static final String CHUNK_WINDOW_MS = "chunk_window_ms";
    public static final String RECORD_TIMESERIE_POINT_TIMESTAMP = "record_timeserie_time";
    public static final String RECORD_TIMESERIE_POINT_VALUE = "record_timeserie_value";


    public TimeSeriesRecord(MetricTimeSeries timeSeries) {
        super(timeSeries.getType());
        this.timeSeries = timeSeries;

        setStringField(FieldDictionary.RECORD_NAME, timeSeries.getName());
        setField(CHUNK_START, FieldType.LONG, timeSeries.getStart());
        setField(CHUNK_END, FieldType.LONG, timeSeries.getEnd());
        setField(CHUNK_SIZE, FieldType.INT, timeSeries.getValues().size());
        setField(CHUNK_WINDOW_MS, FieldType.LONG, timeSeries.getEnd() - timeSeries.getStart());

        timeSeries.attributes().keySet().forEach(key -> {
            setStringField(key, String.valueOf(timeSeries.attribute(key)));
        });
    }

    public MetricTimeSeries getTimeSeries() {
        return timeSeries;
    }


}
