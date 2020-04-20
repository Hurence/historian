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
package com.hurence.logisland.timeseries.converter.compaction;

import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.*;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class BinaryCompactionConverterOfRecord implements Chunker<Record, TimeSeriesRecord>, Serializable {

    private static Logger LOGGER = LoggerFactory.getLogger(BinaryCompactionConverterOfRecord.class.getName());

    private int ddcThreshold = BinaryCompactionUtil.DEFAULT_DDC_THRESHOLD;

    private BinaryCompactionConverterOfRecord(int ddcThreshold) {
        this.ddcThreshold = ddcThreshold;
    }

    /**
     * Compact a related list of records a single chunked one
     *
     * @param records
     * @return
     * @throws ProcessException
     */
    @Override
    public TimeSeriesRecord chunk(List<Record> records) {

        if (records.isEmpty())
            throw new ProcessException("not enough records to build a timeseries, should contain at least 1 records ");

        final MetricTimeSeries timeSeries = buildTimeSeries(records);
        final TimeSeriesRecord chunkrecord = new TimeSeriesRecord(timeSeries);

        // compress chunk into binaries
        byte[] serializedTimeseries = serializeTimeseries(timeSeries);
        chunkrecord.setField(TimeSeriesRecord.CHUNK_VALUE, FieldType.BYTES, serializedTimeseries);
        chunkrecord.setField(TimeSeriesRecord.CHUNK_SIZE_BYTES, FieldType.INT, serializedTimeseries.length);

        return chunkrecord;
    }

    public byte[] serializeTimeseries(final MetricTimeSeries timeSeries) {
        return BinaryCompactionUtil.serializeTimeseries(timeSeries, ddcThreshold);
    }

    private MetricTimeSeries buildTimeSeries(final List<Record> records) {
        final Record first = records.get(0);
        final Record last = records.get(records.size() - 1);
        final String metricType = first.getType();
        final String metricName = first.getField(FieldDictionary.RECORD_NAME).asString();
        final long start = first.getTime().getTime();
        final long end = (last.getTime().getTime() == start) ? start + 1 : start;

        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(metricName, metricType);
        tsBuilder.start(start);
        tsBuilder.end(end);

        // set attributes
        first.getAllFieldsSorted().forEach(field -> {
            if (!field.getName().startsWith("record_"))
                tsBuilder.attribute(field.getName(), field.getRawValue());
        });

        records.forEach(record -> {
            if (record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null) {
                final long timestamp = record.getTime().getTime();
                final double value = record.getField(FieldDictionary.RECORD_VALUE).asDouble();
                tsBuilder.point(timestamp, value);
            }
        });

        return tsBuilder.build();
    }

    /**
     * Reverse operation for chunk operation
     *
     * @param record
     * @return
     * @throws ProcessException
     */
    @Override
    public List<Record> unchunk(final TimeSeriesRecord record) throws IOException {

        final long start = record.getTimeSeries().getStart();
        final long end = record.getTimeSeries().getEnd();
        return BinaryCompactionUtil.unCompressPoints(record.getField(TimeSeriesRecord.CHUNK_VALUE).asBytes(), start, end).stream()
                .map(m -> {

                    long timestamp = m.getTimestamp();
                    double value = m.getValue();

                    Record pointRecord = new StandardRecord(record.getType())
                            .setStringField(FieldDictionary.RECORD_NAME, record.getTimeSeries().getName())
                            .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, timestamp)
                            .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, value);

                    record.getTimeSeries().attributes().keySet().forEach(key -> {
                        pointRecord.setStringField(key, String.valueOf(record.getTimeSeries().attribute(key)));
                    });


                    return pointRecord;
                }).collect(Collectors.toList());
    }

    public static final class Builder {

        private int ddcThreshold = 0;

        public Builder ddcThreshold(final int ddcThreshold) {
            this.ddcThreshold = ddcThreshold;
            return this;
        }

        /**
         * @return a BinaryCompactionConverter as configured
         */
        public BinaryCompactionConverterOfRecord build() {
            return new BinaryCompactionConverterOfRecord(ddcThreshold);
        }
    }
}
