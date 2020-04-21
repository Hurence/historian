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

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class BinaryCompactionUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(BinaryCompactionUtil.class.getName());

    public static int DEFAULT_DDC_THRESHOLD = 0;

    public static byte[] serializeTimeseries(final MetricTimeSeries timeSeries) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(timeSeries.points().iterator(), DEFAULT_DDC_THRESHOLD);
        return Compression.compress(serializedPoints);
    }

    public static byte[] serializeTimeseries(final MetricTimeSeries timeSeries, int ddcThreshold) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(timeSeries.points().iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

//    public MetricTimeSeries buildTimeSeries(String metricName, String metricType,
//                                            long[] timestamps, double[] values, double[] quality) {
//        //TODO check size
//        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(metricName, metricType);
//        tsBuilder.start(timestamps[0]);
//        tsBuilder.end(timestamps[timestamps.length - 1]);
//
//        // set attributes
//        first.getAllFieldsSorted().forEach(field -> {
//            if (!field.getName().startsWith("record_"))
//                tsBuilder.attribute(field.getName(), field.getRawValue());
//        });
//
//        records.forEach(record -> {
//            if (record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null) {
//                final long timestamp = record.getTime().getTime();
//                final double value = record.getField(FieldDictionary.RECORD_VALUE).asDouble();
//                tsBuilder.point(timestamp, value);
//            }
//        });
//
//        return tsBuilder.build();
//    }

    /**
     *
     * @param chunkOfPoints the compressed points
     * @param chunkStart timestamp of the first point in the chunk (required)
     * @param chunkEnd timestamp of the last point in the chunk
     * @return
     * @throws IOException
     */
    public static List<Point> unCompressPoints(byte[] chunkOfPoints, long chunkStart, long chunkEnd) throws IOException {
        try (InputStream decompressed = Compression.decompressToStream(chunkOfPoints)) {
            return ProtoBufMetricTimeSeriesSerializer.from(decompressed, chunkStart, chunkEnd, chunkStart, chunkEnd);
        }
    }

    /**
     * return uncompressed points
     * @param chunkOfPoints the compressed points
     * @param chunkStart the timestamp of the first point of the chunk (needed to uncompress points)
     * @param chunkEnd timestamp of the last point in the chunk
     * @param requestedFrom filter out points with timestamp lower than requestedFrom
     * @param requestedEnd filter out points with timestamp higher than requestedEnd
     * @return
     * @throws IOException
     */
    public static List<Point> unCompressPoints(byte[] chunkOfPoints, long chunkStart, long chunkEnd,
                                        long requestedFrom, long requestedEnd) throws IOException {
        try (InputStream decompressed = Compression.decompressToStream(chunkOfPoints)) {
            return ProtoBufMetricTimeSeriesSerializer.from(decompressed, chunkStart, chunkEnd, requestedFrom, requestedEnd);
        }
    }
}
