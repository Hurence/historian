package com.hurence.timeseries.compaction.protobuf;

import com.hurence.timeseries.converter.serializer.MetricPointWithQualityEmbedded;
import com.hurence.timeseries.modele.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Not thread safe !
 */
public class PointsCompressorWithQuality {

    private static final Logger LOGGER = LoggerFactory.getLogger(PointsCompressorWithQuality.class);

    private long previousDate = -1;
    private long previousDelta = 0;
    private long lastStoredDate = 0;
    private long lastStoredDelta = 0;
    private long previousDrift = 0;
    private int numberOfPointSinceLastDelta = 0;
    private Map<Double, Integer> valueIndex = new HashMap<>();
    private Map<Float, Integer> qualityIndex = new HashMap<>();
    private MetricPointWithQualityEmbedded.Point.Builder point = MetricPointWithQualityEmbedded.Point.newBuilder();
    private MetricPointWithQualityEmbedded.Points.Builder points = MetricPointWithQualityEmbedded.Points.newBuilder();
    private Optional<Float> previousQuality = Optional.empty();
    private boolean shoudBeReset = false;

    private void resetProps() {
        previousDate = -1;
        previousDelta = 0;
        lastStoredDate = 0;
        lastStoredDelta = 0;
        previousDrift = 0;
        numberOfPointSinceLastDelta = 0;
        valueIndex.clear();
        qualityIndex.clear();
        point.clear();
        points.clear();
        previousQuality = Optional.empty();
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public byte[] to(final Iterator<Point> metricDataPoints) {
        return to(metricDataPoints, 0);
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public byte[] to(final Iterator<Point> metricDataPoints, long ddcThreshold) {
        if (shoudBeReset) {
            resetProps();
        }
        int index = 0;
        while (metricDataPoints.hasNext()) {
            Point p = metricDataPoints.next();
            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }
            point.clear();
            long currentTimestamp = p.getTimestamp();
            if (previousDate == -1) {
                lastStoredDate = currentTimestamp;
            }
            //Add value or index, if the value already exists
            setValueOrRefIndexOnPoint(valueIndex, index, p.getValue(), point);
            //setQualityIfNeeded
            float currentQuality = getQualityOfPoint(p);
            addQualityToPointsIfNeeded(qualityIndex, points, previousQuality, index, currentQuality);
            previousQuality = Optional.of(currentQuality);

            long delta = calculDelta(previousDate, currentTimestamp);

            boolean isAlmostEquals = almostEquals(previousDelta, delta, ddcThreshold);
            long drift = 0;
            if (isAlmostEquals) {
                drift = calculateDrift(currentTimestamp, lastStoredDate, numberOfPointSinceLastDelta, lastStoredDelta);;
                if (noDrift(drift, ddcThreshold, numberOfPointSinceLastDelta) && drift >= 0) {
                    numberOfPointSinceLastDelta += 1;
                } else {
                    saveTimestampOffsetForPoint(p, delta);
                }
            } else {
                saveTimestampOffsetForPoint(p, delta);
            }
            points.addP(point.build());
            //set current as former previous date
            previousDate = currentTimestamp;
            previousDelta = delta;
            previousDrift = drift;
            index++;
        }
        return points.build().toByteArray();
    }

    private void saveTimestampOffsetForPoint(Point p, long delta) {
        long timeStampOffset = delta;
        if (numberOfPointSinceLastDelta > 0 && delta > previousDrift) {
            timeStampOffset = delta - previousDrift;
            setBPTimeStamp(point, timeStampOffset);
        } else {
            setTimeStamp(point, timeStampOffset);
        }
        lastStoredDate = p.getTimestamp();
        lastStoredDelta = timeStampOffset;
        numberOfPointSinceLastDelta = 0;
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
     * Set value as a base point delta timestamp
     * A base point delta timestamp is a corrected timestamp to the actual timestamp.
     *
     * @param point          the point
     * @param timestampDelta the timestamp delta
     */
    private static void setBPTimeStamp(MetricPointWithQualityEmbedded.Point.Builder point, long timestampDelta) {
        if (safeLongToUInt(timestampDelta)) {
            point.setTintBP((int) timestampDelta);
        } else {
            point.setTlongBP(timestampDelta);
        }
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

    private static void addQualityToPointsIfNeeded(Map<Float, Integer> qualityIndex, MetricPointWithQualityEmbedded.Points.Builder points, Optional<Float> previousQuality, int index, float currentQuality) {
//       ajout pour le premier point ou si la qualité a changer
        if (!previousQuality.isPresent() || currentQuality != previousQuality.get()) {
            MetricPointWithQualityEmbedded.Quality q = buildQuality(qualityIndex, index, currentQuality);
            points.addQ(q);
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
        q.setPointIndex(index);
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
