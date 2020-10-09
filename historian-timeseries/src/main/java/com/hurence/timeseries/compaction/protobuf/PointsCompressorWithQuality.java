package com.hurence.timeseries.compaction.protobuf;

import com.hurence.timeseries.converter.serializer.ChunkProtocolBuffers;
import com.hurence.timeseries.model.Measure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Not thread safe !
 */
public class PointsCompressorWithQuality {

    private static final Logger LOGGER = LoggerFactory.getLogger(PointsCompressorWithQuality.class);
    private static final BigDecimal MIN_QUALITY = BigDecimal.valueOf(0f);
    private static final BigDecimal MAX_QUALITY = BigDecimal.valueOf(1f);

    private long previousDate = -1;
    private long previousDelta = 0;
    private long lastStoredDate = 0;
    private long lastStoredDelta = 0;
    private long previousDrift = 0;
    private int numberOfPointSinceLastDelta = 0;
    private Map<Double, Integer> valueIndex = new HashMap<>();
    private Map<Float, Integer> qualityIndex = new HashMap<>();
    private ChunkProtocolBuffers.Point.Builder point = ChunkProtocolBuffers.Point.newBuilder();
    private ChunkProtocolBuffers.Chunk.Builder points = ChunkProtocolBuffers.Chunk.newBuilder();
    private Optional<Float> previousStoredQuality = Optional.empty();
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
        previousStoredQuality = Optional.empty();
    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points (expected te be already sorted !)
     * @return the serialized points as byte[]
     */
    public byte[] to(final Iterator<Measure> metricDataPoints, float diffAcceptedForQuality, long ddcThreshold) {
        if (shoudBeReset) {
            resetProps();
        }
        int index = 0;
        while (metricDataPoints.hasNext()) {
            Measure p = metricDataPoints.next();
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
            float qualityOfPoint = getQualityOfPoint(p);
            addQualityToPointsIfNeeded(index, qualityOfPoint, diffAcceptedForQuality);


            long delta = calculDelta(currentTimestamp);

            boolean isAlmostEquals = almostEquals(delta, ddcThreshold);
            long drift = 0;
            if (isAlmostEquals) {
                drift = calculateDrift(currentTimestamp);;
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

    private void saveTimestampOffsetForPoint(Measure p, long delta) {
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
     * @return
     */
    private long calculateDrift(long timestamp) {
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
    private static void setBPTimeStamp(ChunkProtocolBuffers.Point.Builder point, long timestampDelta) {
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
     * @param offset         the current offset
     * @param ddcThreshold   the threshold for equality
     * @return true if set offsets are equals using the threshold
     */
    private boolean almostEquals(long offset, long ddcThreshold) {
        //check the deltas
        long diff = Math.abs(offset - previousDelta);
        return (diff <= ddcThreshold);
    }

    private void addQualityToPointsIfNeeded(int index, float currentQuality, float diffAcceptedForQuality) {
//       ajout pour le premier point ou si la qualité a changer
        if (!previousStoredQuality.isPresent()) {
            addQualityToPoint(index, currentQuality);
            return;
        }
        BigDecimal previousStoredQualityBigDec = BigDecimal.valueOf(previousStoredQuality.get());
        BigDecimal currentQualityBigDec = BigDecimal.valueOf(currentQuality);
        BigDecimal thresholdBigDec = BigDecimal.valueOf(diffAcceptedForQuality);
        //store quality if it has changed significantly or if it is min or max and changed.
        if (!isFloatsAlmostEquals(previousStoredQualityBigDec, currentQualityBigDec, thresholdBigDec) ||
                (qualityIsMinOrMax(currentQualityBigDec) &&
                        previousStoredQualityBigDec.compareTo(currentQualityBigDec) != 0)) {
            addQualityToPoint(index, currentQuality);
        } else {
            LOGGER.trace("skipping quality of point index {} with quality {}, stored quality is {}",
                    index, currentQuality, previousStoredQuality.get());
        }
    }

    private void addQualityToPoint(int index, float currentQuality) {
        LOGGER.trace("Saved quality of point index {} with quality {}", index, currentQuality);
        ChunkProtocolBuffers.Quality q = buildQuality(qualityIndex, index, currentQuality);
        points.addQ(q);
        previousStoredQuality = Optional.of(currentQuality);
    }

    private boolean qualityIsMinOrMax(BigDecimal quality) {
        return quality.compareTo(MIN_QUALITY) == 0 || quality.compareTo(MAX_QUALITY) == 0;
    }

    public static boolean isFloatsAlmostEquals(BigDecimal float1, BigDecimal float2, BigDecimal threshold) {
        int diff = float1.subtract(float2).abs().compareTo(threshold);
        return diff <= 0;
    }

    private static float getQualityOfPoint(Measure p) {
        if (p.hasQuality()) {
            return p.getQuality();
        } else {
            return -1;
        }
    }

    private static ChunkProtocolBuffers.Quality buildQuality(
            Map<Float, Integer> qualityIndex,
            int index, float quality) {
        //build value index
        ChunkProtocolBuffers.Quality.Builder q = ChunkProtocolBuffers.Quality.newBuilder();
        if (qualityIndex.containsKey(quality)) {
            q.setVIndex(qualityIndex.get(quality));
        } else {
            qualityIndex.put(quality, index);
            q.setV(quality);
        }
        q.setPointIndex(index);
        return q.build();
    }

    private long calculDelta(long currentTimestamp) {
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
    private static void setValueOrRefIndexOnPoint(Map<Double, Integer> index, int currentPointIndex, double value, ChunkProtocolBuffers.Point.Builder point) {
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
    private static void setTimeStamp(ChunkProtocolBuffers.Point.Builder point, long timestampDelta) {
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
