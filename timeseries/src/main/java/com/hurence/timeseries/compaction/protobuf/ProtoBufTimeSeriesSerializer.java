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


import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.modele.DoubleList;
import com.hurence.timeseries.modele.LongList;
import com.hurence.timeseries.converter.serializer.MetricProtocolBuffers;
import com.hurence.timeseries.modele.PointImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Class to easily convert the protocol buffer into Point<Long,Double>
 *
 * @author f.lautenschlager
 */
public final class ProtoBufTimeSeriesSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufTimeSeriesSerializer.class);

    /**
     * Private constructor
     */
    private ProtoBufTimeSeriesSerializer() {
        //utility class
    }

    /**
     * return the uncompressed points (compressed byte array)
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     */
    public static List<PointImpl> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd) throws IOException, IllegalArgumentException {
        return from(decompressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd);
    }
    /**
     * return the uncompressed points (compressed byte array)
     *
     * @param decompressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart   the start of the time series
     * @param timeSeriesEnd     the end of the time series
     * @param from              including points from
     * @param to                including points to
     */
    public static List<PointImpl> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to) throws IOException, IllegalArgumentException {
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
            //TODO add possibility to choose ddcThreshold
            MetricProtocolBuffers.Points protocolBufferPoints = MetricProtocolBuffers.Points.parseFrom(decompressedBytes);

            List<MetricProtocolBuffers.Point> pList = protocolBufferPoints.getPList();
            List<PointImpl> pointsToReturn = new ArrayList<>();

            int size = pList.size();

            long lastDelta = protocolBufferPoints.getDdc();
            long calculatedPointDate = timeSeriesStart;

            double value;


            for (int i = 0; i < size; i++) {

                MetricProtocolBuffers.Point p = pList.get(i);


                //Decode the time
                if (i > 0) {
                    lastDelta = getTimestamp(p, lastDelta);
                    calculatedPointDate += lastDelta;
                }
                //only add the point if it is within the date
                if (calculatedPointDate >= from) {
                    //Check if the point refers to an index
                    if (p.hasVIndex()) {
                        value = pList.get(p.getVIndex()).getV();
                    } else {
                        value = p.getV();
                    }
                    if (calculatedPointDate > to) {
                        LOGGER.debug("remaining {} points are skipped after t={}", size - i, calculatedPointDate);
                        return pointsToReturn;
                    }
                    pointsToReturn.add(new PointImpl(calculatedPointDate, value));
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
     * @param metricDataPoints - the list with points
     * @return the serialized points as byte[]
     */
    public static byte[] to(Iterator<PointImpl> metricDataPoints) {
        return to(metricDataPoints, 0);
    }


    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points
     * @param ddcThreshold     - the aberration threshold for the deltas
     * @return the serialized points as byte[]
     */
    public static byte[] to(final Iterator<PointImpl> metricDataPoints, final int ddcThreshold) {

        if (ddcThreshold < 0) {
            throw new IllegalArgumentException("DDC Threshold must not be lower than 0. Current value is: " + ddcThreshold);
        }

        long previousDate = -1;
        long previousDelta = 0;
        long previousDrift = 0;

        long startDate = 0;
        long lastStoredDate = 0;
        long delta = 0;
        long lastStoredDelta = 0;

        int numberOfPointSinceLastDelta = 0;

        Map<Double, Integer> valueIndex = new HashMap<>();

        MetricProtocolBuffers.Point.Builder point = MetricProtocolBuffers.Point.newBuilder();
        MetricProtocolBuffers.Points.Builder points = MetricProtocolBuffers.Points.newBuilder();


        int index = 0;
        while (metricDataPoints.hasNext()) {

            PointImpl p = metricDataPoints.next();
            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }

            point.clear();
            long currentTimestamp = p.getTimestamp();

            //Add value or index, if the value already exists
            setValueOrRefIndexOnPoint(valueIndex, index, p.getValue(), point);

            if (previousDate == -1) {
                // set lastStoredDate to the value of the first timestamp
                lastStoredDate = currentTimestamp;
                startDate = currentTimestamp;
            } else {
                delta = currentTimestamp - previousDate;
            }


            //Last point
            if (!metricDataPoints.hasNext()) {
                handleLastPoint(ddcThreshold, startDate, point, points, currentTimestamp);
                break;
            }


            //We have normal point
            boolean isAlmostEquals = almostEquals(previousDelta, delta, ddcThreshold);
            long drift = 0;

            //The deltas of the timestamps are almost equals (delta < ddcThreshold)
            if (isAlmostEquals) {
                //calculate the drift to the actual timestamp
                drift = calculateDrift(currentTimestamp, lastStoredDate, numberOfPointSinceLastDelta, lastStoredDelta);
            }

            if (isAlmostEquals && noDrift(drift, ddcThreshold, numberOfPointSinceLastDelta) && drift >= 0) {
                points.addP(point.build());
                numberOfPointSinceLastDelta += 1;
            } else {
                long timeStampOffset = delta;
                //If the previous offset was not stored, correct the following delta using the calculated drift
                if (numberOfPointSinceLastDelta > 0 && delta > previousDrift) {
                    timeStampOffset = delta - previousDrift;
                    setBPTimeStamp(point, timeStampOffset);
                } else {
                    setTimeStamp(point, timeStampOffset);
                }

                //Store offset
                points.addP(point.build());
                //reset the offset counter
                numberOfPointSinceLastDelta = 0;
                lastStoredDate = p.getTimestamp();
                lastStoredDelta = timeStampOffset;

            }
            //set current as former previous date
            previousDrift = drift;
            previousDelta = delta;
            previousDate = currentTimestamp;

            index++;
        }
        //set the ddc value
        points.setDdc(ddcThreshold);
        return points.build().toByteArray();
    }

    /**
     * Gets the time stamp from the point.
     *
     * @param p          the protocol buffers point
     * @param lastOffset the last stored offset
     * @return the time stamp of the point or the last offset if the point do not have any information about the time stamp
     */
    private static long getTimestamp(final MetricProtocolBuffers.Point p, final long lastOffset) {
        //Normal delta
        if (p.hasTint() || p.hasTlong()) {
            return p.getTint() + p.getTlong();//Todo in my opinion either getTint either getTint,
            //todo here it works because one is 0 if not assigned but this is not logical
        }
        //Base point delta
        if (p.hasTintBP() || p.hasTlongBP()) {
            return p.getTintBP() + p.getTlongBP();//todo same
        }
        return lastOffset;
    }

    /**
     * Handles the last point of a time series.
     * We always store the first and the last point as supporting points actualPoints[Last] == serializedPoints[Last]
     * This means that we serialized point so that timestamps of first and last point are not degraded.
     *
     * @param ddcThreshold     the ddc threshold
     * @param startDate        the start date
     * @param point            the current point
     * @param points           the protocol buffer point
     * @param currentTimestamp the current time stamp
     */
    private static void handleLastPoint(int ddcThreshold, long startDate,
                                        MetricProtocolBuffers.Point.Builder point,
                                        MetricProtocolBuffers.Points.Builder points,
                                        long currentTimestamp) {
        long calcPoint = calculateTimeStamp(startDate, points.getPList(), ddcThreshold);
        //Calc offset
        long deltaToLastTimestamp = currentTimestamp - calcPoint;

        //everything okay
        if (deltaToLastTimestamp >= 0) {
            setTimeStamp(point, deltaToLastTimestamp);
            points.addP(point);
        } else {
            //we have to rearrange the points as we are already behind the actual end timestamp
            rearrangePoints(startDate, currentTimestamp, deltaToLastTimestamp, ddcThreshold, points, point);
        }
    }

    /**
     * Sets the given value or if the value exists in the index, the index position as value of the point.
     *
     * @param index             the map holding the values and the indices
     * @param currentPointIndex the current index position
     * @param value             the current value
     * @param point             the current point builder
     */
    private static void setValueOrRefIndexOnPoint(Map<Double, Integer> index, int currentPointIndex, double value, MetricProtocolBuffers.Point.Builder point) {
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
    private static void setTimeStamp(MetricProtocolBuffers.Point.Builder point, long timestampDelta) {
        if (safeLongToUInt(timestampDelta)) {
            point.setTint((int) timestampDelta);
        } else {
            point.setTlong(timestampDelta);
        }
    }

    /**
     * Set value as a base point delta timestamp
     * A base point delta timestamp is a corrected timestamp to the actual timestamp.
     *
     * @param point          the point
     * @param timestampDelta the timestamp delta
     */
    private static void setBPTimeStamp(MetricProtocolBuffers.Point.Builder point, long timestampDelta) {
        if (safeLongToUInt(timestampDelta)) {
            point.setTintBP((int) timestampDelta);
        } else {
            point.setTlongBP(timestampDelta);
        }
    }

    /**
     * Rearranges the serialized points in order to fit the points within the start and end date of the actual time series
     *
     * @param startDate           the start date
     * @param currentTimestamp    the current timestamp
     * @param deltaToEndTimestamp the delta to the end timestamp
     * @param ddcThreshold        the ddc threshold
     * @param points              the serialized points
     * @param point               the serialized point
     */
    private static void rearrangePoints(final long startDate, final long currentTimestamp,
                                        final long deltaToEndTimestamp, final int ddcThreshold,
                                        final MetricProtocolBuffers.Points.Builder points,
                                        final MetricProtocolBuffers.Point.Builder point) {
        //break the offset down on all points
        long avgPerDelta = (long) Math.ceil((double) deltaToEndTimestamp * -1 + ddcThreshold / (double) (points.getPCount() - 1));

        for (int i = 1; i < points.getPCount(); i++) {
            MetricProtocolBuffers.Point mod = points.getP(i);
            long t = getT(mod);

            //check if can correct the deltas
            if (deltaToEndTimestamp < 0) {
                long newOffset;

                if (deltaToEndTimestamp + avgPerDelta > 0) {
                    avgPerDelta = deltaToEndTimestamp * -1;
                }

                //if we have a t value
                if (t > avgPerDelta) {
                    newOffset = t - avgPerDelta;
                    MetricProtocolBuffers.Point.Builder modPoint = mod.toBuilder();
                    setT(modPoint, newOffset);
                    mod = modPoint.build();
                }

            }
            points.setP(i, mod);
        }


        //Done
        long arrangedPoint = calculateTimeStamp(startDate, points.getPList(), ddcThreshold);

        long storedOffsetToEnd = currentTimestamp - arrangedPoint;
        if (storedOffsetToEnd < 0) {
            LOGGER.warn("Stored offset is negative. Setting to 0. But that is an error.");
            storedOffsetToEnd = 0;
        }

        setBPTimeStamp(point, storedOffsetToEnd);

        points.addP(point);
    }


    /**
     * Sets the new t for the point. Checks which t was set.
     *
     * @param builder the point builder
     * @param delta   the new delta to set on the given point
     */
    private static void setT(MetricProtocolBuffers.Point.Builder builder, long delta) {
        if (safeLongToUInt(delta)) {
            if (builder.hasTintBP()) {
                builder.setTintBP((int) delta);
            }
            if (builder.hasTint()) {
                builder.setTint((int) delta);
            }
        } else {
            if (builder.hasTlongBP()) {
                builder.setTlongBP(delta);
            }
            if (builder.hasTlong()) {
                builder.setTlong(delta);
            }
        }

    }

    /**
     * @param point the current point
     * @return the value of t
     */
    private static long getT(MetricProtocolBuffers.Point point) {
        //only one is set, others are zero
        return point.getTlongBP() + point.getTlong() + point.getTint() + point.getTintBP();
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

    /**
     * @param startDate    the first time stamp
     * @param pList        the list with serialized points
     * @param ddcThreshold the threshold of the ddc
     * @return the calculated timestamp using the ddc threshold
     */
    private static long calculateTimeStamp(long startDate, List<MetricProtocolBuffers.Point> pList, long ddcThreshold) {

        long lastDelta = ddcThreshold;
        long calculatedPointDate = startDate;

        for (int i = 1; i < pList.size(); i++) {
            MetricProtocolBuffers.Point p = pList.get(i);
            lastDelta = getTimestamp(p, lastDelta);
            calculatedPointDate += lastDelta;
        }
        return calculatedPointDate;
    }

    /**
     * @param drift                    the calculated drift (difference between calculated and actual time stamp)
     * @param ddcThreshold             the ddc threshold
     * @param numberOfPointSinceLastDelta times since a delta was stored
     * @return true if the drift is below ddcThreshold/2, otherwise false
     */
    private static boolean noDrift(long drift, long ddcThreshold, long numberOfPointSinceLastDelta) {
        return numberOfPointSinceLastDelta == 0 || drift == 0 || drift < (ddcThreshold / 2);
    }


    /**
     * Calculates the drift between the given timestamp and the reconstructed time stamp
     *
     * @param timestamp           the actual time stamp
     * @param lastStoredDate      the last stored date
     * @param numberOfPointSinceLastDelta the times no delta was stored
     * @param lastStoredDelta     the last stored delta
     * @return
     */
    private static long calculateDrift(long timestamp, long lastStoredDate, int numberOfPointSinceLastDelta, long lastStoredDelta) {
        long calculatedMaxOffset = lastStoredDelta * (numberOfPointSinceLastDelta + 1);
        return lastStoredDate + calculatedMaxOffset - timestamp;
    }

    /**
     * Check if two deltas are almost equals.
     * <p>
     * abs(offset - previousOffset) <= aberration
     * </p>
     *
     * @param previousOffset the previous offset
     * @param offset         the current offset
     * @param ddcThreshold   the threshold for equality
     * @return true if set offsets are equals using the threshold
     */
    private static boolean almostEquals(long previousOffset, long offset, long ddcThreshold) {
        //check the deltas
        long diff = Math.abs(offset - previousOffset);
        return (diff <= ddcThreshold);
    }

}

