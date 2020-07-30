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


import com.hurence.timeseries.compaction.Compression;
import com.hurence.timeseries.modele.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Class to easily convert the protocol buffer into List<Point>
 *
 */
public final class ProtoBufTimeSeriesCurrentSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(   ProtoBufTimeSeriesCurrentSerializer.class);

    private static final float DEFAULT_QUALITY_EQUALS = 0.1f;
    /**
     * Private constructor
     */
    private ProtoBufTimeSeriesCurrentSerializer() {
        //utility class
    }

    /**
     * return the uncompressed points (compressed byte array)
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     */
    public static List<Point> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, byte[] chunkOfPoints) throws IOException, IllegalArgumentException {
        return from(decompressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, chunkOfPoints);
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
    public static List<Point> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd
            , long from, long to, byte[] chunkOfPoints) throws IOException, IllegalArgumentException {
        try {
            return ProtoBufTimeSeriesWithQualitySerializer.from(decompressedBytes, timeSeriesStart, timeSeriesEnd, from, to);
        } catch (IllegalArgumentException ex) {
            LOGGER.trace("could not uncompress using algo with quality will try with old algorithm");
            /*decompressedBytes.reset();*/
            InputStream newDecompressedBytes = Compression.decompressToStream(chunkOfPoints);
            return ProtoBufTimeSeriesSerializer.from(newDecompressedBytes, timeSeriesStart, timeSeriesEnd, from, to);
        }
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(List<Point> metricDataPoints) {
        return to(metricDataPoints, 0);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(Iterator<Point> metricDataPoints) {
        return to(metricDataPoints, 0);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final List<Point> metricDataPoints, long ddcThreshold) {
        return to(metricDataPoints.iterator(), ddcThreshold);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<Point> metricDataPoints, long ddcThreshold) {
        return to(metricDataPoints, DEFAULT_QUALITY_EQUALS, ddcThreshold);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<Point> metricDataPoints, float diffAcceptedForQuality, long ddcThreshold) {
        return ProtoBufTimeSeriesWithQualitySerializer.to(metricDataPoints, diffAcceptedForQuality, ddcThreshold);
    }

}

