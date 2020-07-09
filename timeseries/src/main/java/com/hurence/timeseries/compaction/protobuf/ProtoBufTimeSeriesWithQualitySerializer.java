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


import com.hurence.timeseries.converter.serializer.MetricPointWithQualityEmbedded;
import com.hurence.timeseries.modele.Point;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.timeseries.modele.PointWithQualityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Class to easily convert the protocol buffer into List<Point>
 *
 */
public final class ProtoBufTimeSeriesWithQualitySerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufTimeSeriesWithQualitySerializer.class);

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
    public static List<Point> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd) throws IOException, IllegalArgumentException {
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
    public static List<Point> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to) throws IOException, IllegalArgumentException {
        LOGGER.debug("from - timeSeriesStart={} timeSeriesEnd={} to={} from={}", timeSeriesStart, timeSeriesEnd, to, from);
        if (from == -1 || to == -1) {
            throw new IllegalArgumentException("FROM or TO have to be >= 0");
        }

        //if to is left of the time series, we have no points to return
        if (to < timeSeriesStart) {
            LOGGER.debug("error to={} is lower than timeSeriesStart={}", to, timeSeriesStart);
            return Collections.emptyList();
        }
        //if from is greater  to, we have nothing to return
        if (from > to) {
            LOGGER.debug("error from={} is greater than to={}", from, to);
            return Collections.emptyList();
        }

        //if from is right of the time series we have nothing to return
        if (from > timeSeriesEnd) {
            LOGGER.debug("error from={} is greater than timeSeriesEnd={}", from, timeSeriesEnd);
            return Collections.emptyList();
        }

        try {
            MetricPointWithQualityEmbedded.Points protocolBufferPoints = MetricPointWithQualityEmbedded.Points.parseFrom(decompressedBytes);

            List<MetricPointWithQualityEmbedded.Point> pList = protocolBufferPoints.getPList();
            List<MetricPointWithQualityEmbedded.Quality> qList = protocolBufferPoints.getQList();
            ListIterator<MetricPointWithQualityEmbedded.Quality> qListIterator = qList.listIterator();
            if (!qListIterator.hasNext()) throw new IllegalArgumentException("qList should not be empty. Bad or icompatible compressedBytes !");
            List<Point> pointsToReturn = new ArrayList<>();
            int size = pList.size();
            float currentQuality = getQuality(qList, qListIterator.next());
            Optional<Integer> indexForNextQuality = findIndexForNextQuality(qListIterator);
            long calculatedPointDate = timeSeriesStart;
            for (int i = 0; i < size; i++) {
                MetricPointWithQualityEmbedded.Point p = pList.get(i);
                //Decode the time for point that is not the first
                if (i > 0) {
                    calculatedPointDate = getCalculatedPointTimestamp(calculatedPointDate, p);
                }
                if (indexForNextQuality.isPresent() && indexForNextQuality.get() == i) {
                    currentQuality = getQuality(qList, qListIterator.next());
                    indexForNextQuality = findIndexForNextQuality(qListIterator);
                }
                //only add the point if it is within the date
                if (calculatedPointDate >= from) {
                    //Check if the point refers to an index
                    if (calculatedPointDate > to) {
                        LOGGER.debug("remaining {} points are skipped after t={}", size - i, calculatedPointDate);
                        return pointsToReturn;
                    }
                    double value = getValue(pList, p);
                    if (currentQuality == -1f) {
                        pointsToReturn.add(new PointImpl(calculatedPointDate, value));
                    } else {
                        pointsToReturn.add(new PointWithQualityImpl(calculatedPointDate, value, currentQuality));
                    }
                } else {
                    LOGGER.debug("not adding point at t={}", calculatedPointDate);
                }
            }
            return pointsToReturn;
        } catch (IOException e) {
            LOGGER.info("Could not decode protocol buffers points");
            throw e;
        }
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final List<Point> metricDataPoints) {
        return to(metricDataPoints.iterator());
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<Point> metricDataPoints) {
        long previousDate = -1;
        Map<Double, Integer> valueIndex = new HashMap<>();
        Map<Float, Integer> qualityIndex = new HashMap<>();
        MetricPointWithQualityEmbedded.Point.Builder point = MetricPointWithQualityEmbedded.Point.newBuilder();
        MetricPointWithQualityEmbedded.Points.Builder points = MetricPointWithQualityEmbedded.Points.newBuilder();
        Optional<Float> previousQuality = Optional.empty();
        int index = 0;
        while (metricDataPoints.hasNext()) {
            Point p = metricDataPoints.next();
            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }
            point.clear();
            long currentTimestamp = p.getTimestamp();
            //Add value or index, if the value already exists
            setValueOrRefIndexOnPoint(valueIndex, index, p.getValue(), point);
            //setQualityIfNeeded
            float currentQuality = getQualityOfPoint(p);
            addQualityToPointsIfNeeded(qualityIndex, points, previousQuality, index, currentQuality);
            previousQuality = Optional.of(currentQuality);

            long delta = calculDelta(previousDate, currentTimestamp);
            if (delta != 0) {
                setTimeStamp(point, delta);
            }
            points.addP(point.build());
            //set current as former previous date
            previousDate = currentTimestamp;
            index++;
        }
        return points.build().toByteArray();
    }

    private static Optional<Integer> findIndexForNextQuality(ListIterator<MetricPointWithQualityEmbedded.Quality> qListIterator) {
        if (qListIterator.hasNext()) {
            MetricPointWithQualityEmbedded.Quality nextQuality = qListIterator.next();
            qListIterator.previous();
            return Optional.of(nextQuality.getTIndex());
        }
        return Optional.empty();
    }

    private static float getQuality(List<MetricPointWithQualityEmbedded.Quality> qList, MetricPointWithQualityEmbedded.Quality q) {
        float quality;
        if (q.hasVIndex()) {
            quality = qList.get(q.getVIndex()).getV();
        } else {
            quality = q.getV();
        }
        return quality;
    }


    private static double getValue(List<MetricPointWithQualityEmbedded.Point> pList, MetricPointWithQualityEmbedded.Point p) {
        double value;
        if (p.hasVIndex()) {
            value = pList.get(p.getVIndex()).getV();
        } else {
            value = p.getV();
        }
        return value;
    }

    private static long getCalculatedPointTimestamp(long calculatedPointDate, MetricPointWithQualityEmbedded.Point p) {
        long offset = getOffsetOfPoint(p).orElse(0L);
        return calculatedPointDate + offset;
    }

    private static void addQualityToPointsIfNeeded(Map<Float, Integer> qualityIndex, MetricPointWithQualityEmbedded.Points.Builder points, Optional<Float> previousQuality, int index, float currentQuality) {
//       ajout pour le premier point ou si la qualité a changer
        if (!previousQuality.isPresent() || currentQuality != previousQuality.get()) {
            MetricPointWithQualityEmbedded.Quality q = buildQuality(qualityIndex, index, currentQuality);
            points.addQ(q);
        }
    }

    private static Optional<Float> convertToOptional(float quality) {
        if (quality == -1) {
            return Optional.empty();
        } else {
            return Optional.of(quality);
        }
    }

    private static float getQualityOfPoint(Point p) {
        if (p.hasQuality()) {
            return p.getQuality();
        } else {
            return -1;
        }
    }

    private static MetricPointWithQualityEmbedded.Quality buildQuality(
            Map<Float, Integer> qualityIndex,
            int index, float quality) {
        //build value index
        MetricPointWithQualityEmbedded.Quality.Builder q = MetricPointWithQualityEmbedded.Quality.newBuilder();
        if (qualityIndex.containsKey(quality)) {
            q.setVIndex(qualityIndex.get(quality));
        } else {
            qualityIndex.put(quality, index);
            q.setV(quality);
        }
        q.setTIndex(index);
        return q.build();
    }

    private static long calculDelta(long previousDate, long currentTimestamp) {
        long delta = 0;
        if (previousDate != -1) {
            delta = currentTimestamp - previousDate;
        }
        return delta;
    }

    /**
     * Gets the time stamp from the point.
     *
     * @param p          the protocol buffers point
     * @return the time stamp of the point or the last offset if the point do not have any information about the time stamp
     */
    private static Optional<Long> getOffsetOfPoint(final MetricPointWithQualityEmbedded.Point p) {
        if (p.hasTint()) return Optional.of((long) p.getTint());
        if (p.hasTlong()) return Optional.of(p.getTlong());
        return Optional.empty();
    }


    /**
     * Sets the given value or if the value exists in the index, the index position as value of the point.
     *
     * @param index             the map holding the values and the indices
     * @param currentPointIndex the current index position
     * @param value             the current value
     * @param point             the current point builder
     */
    private static void setValueOrRefIndexOnPoint(Map<Double, Integer> index, int currentPointIndex, double value, MetricPointWithQualityEmbedded.Point.Builder point) {
        //build value index
        if (index.containsKey(value)) {
            point.setVIndex(index.get(value));
        } else {
            index.put(value, currentPointIndex);
            point.setV(value);
        }
    }

    /**
     * Set value as normal delta timestamp
     *
     * @param point          the point
     * @param timestampDelta the timestamp delta
     */
    private static void setTimeStamp(MetricPointWithQualityEmbedded.Point.Builder point, long timestampDelta) {
        if (safeLongToUInt(timestampDelta)) {
            point.setTint((int) timestampDelta);
        } else {
            point.setTlong(timestampDelta);
        }
    }

    /**
     * Checks if the given long value could be cast to an integer
     *
     * @param value the long value
     * @return true if value < INTEGER.MAX_VALUE
     */
    private static boolean safeLongToUInt(long value) {
        return !(value < 0 || value > Integer.MAX_VALUE);
    }

}

