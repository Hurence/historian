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
package com.hurence.timeseries.compaction.protobuf;


import com.hurence.timeseries.model.Measure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Class to easily convert the protocol buffer into List<Measure>
 *
 */
public final class ProtoBufTimeSeriesWithQualitySerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(   ProtoBufTimeSeriesWithQualitySerializer.class);

    private static final float DEFAULT_QUALITY_EQUALS = 0.1f;
    /**
     * Private constructor
     */
    private ProtoBufTimeSeriesWithQualitySerializer() {
        //utility class
    }

    /**
     * return the uncompressed points (compressed byte array)
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     */
    public static TreeSet<Measure> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd) throws IOException, IllegalArgumentException {
        return from(decompressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd);
    }
    /**
     * return the uncompressed points (compressed byte array)
     * The quality value in compressedByte is supposed to be -1 if there no quality. This
     * is safe because we expect quality to be a float between 0 and 1 if it is present.
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     * @param from              including points from
     * @param to                including points to
     */
    public static TreeSet<Measure> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to) throws IOException, IllegalArgumentException {
        PointsUnCompressorWithQuality unCompressor = new PointsUnCompressorWithQuality();
        return unCompressor.from(decompressedBytes, timeSeriesStart, timeSeriesEnd, from, to);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataMeasures - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(List<Measure> metricDataMeasures) {
        return to(metricDataMeasures, 0);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(Iterator<Measure> metricDataPoints) {
        return to(metricDataPoints, 0);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataMeasures - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final List<Measure> metricDataMeasures, long ddcThreshold) {
        return to(metricDataMeasures.iterator(), ddcThreshold);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<Measure> metricDataPoints, long ddcThreshold) {
        return to(metricDataPoints, DEFAULT_QUALITY_EQUALS, ddcThreshold);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<Measure> metricDataPoints, float diffAcceptedForQuality, long ddcThreshold) {
        PointsCompressorWithQuality compressor = new PointsCompressorWithQuality();
        return compressor.to(metricDataPoints, diffAcceptedForQuality, ddcThreshold);
    }

}

