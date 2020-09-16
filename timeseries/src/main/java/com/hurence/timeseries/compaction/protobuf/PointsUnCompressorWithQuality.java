package com.hurence.timeseries.compaction.protobuf;

import com.hurence.timeseries.converter.serializer.ChunkProtocolBuffers;
import com.hurence.timeseries.model.Measure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Not thread safe !
 */
public class PointsUnCompressorWithQuality {

    private static final Logger LOGGER = LoggerFactory.getLogger(PointsUnCompressorWithQuality.class);

    long calculatedPointDate = 0;
    long lastOffSet = 0;
    float currentQuality = -1;
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
    public List<Measure> from(final InputStream decompressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to) throws IOException, IllegalArgumentException {
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
            ChunkProtocolBuffers.Chunk protocolBufferPoints = ChunkProtocolBuffers.Chunk.parseFrom(decompressedBytes);

            List<ChunkProtocolBuffers.Point> pList = protocolBufferPoints.getPList();
            List<ChunkProtocolBuffers.Quality> qList = protocolBufferPoints.getQList();
            ListIterator<ChunkProtocolBuffers.Quality> qListIterator = qList.listIterator();
            if (!qListIterator.hasNext()) throw new IllegalArgumentException("qList should not be empty. Bad or icompatible compressedBytes !");
            List<Measure> pointsToReturn = new ArrayList<>();
            int size = pList.size();
            currentQuality = getQuality(qList, qListIterator.next());
            Optional<Integer> indexForNextQuality = findIndexForNextQuality(qListIterator);
            calculatedPointDate = timeSeriesStart;
            setLastOffsetAccordingToDdcThreshold(protocolBufferPoints);
            for (int i = 0; i < size; i++) {
                ChunkProtocolBuffers.Point p = pList.get(i);
                //Decode the time for point that is not the first
                if (i > 0) {
                    lastOffSet = getOffset(p).orElse(lastOffSet);
                    calculatedPointDate += lastOffSet;
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
                        pointsToReturn.add( Measure.fromValue(calculatedPointDate, value));
                    } else {
                        pointsToReturn.add( Measure.fromValueAndQuality(calculatedPointDate, value, currentQuality));
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

    private void setLastOffsetAccordingToDdcThreshold(ChunkProtocolBuffers.Chunk protocolBufferPoints) {
        if (protocolBufferPoints.hasDdc()) {
            lastOffSet = protocolBufferPoints.getDdc();
        } else {
            lastOffSet = 0;
        }
    }


    private static Optional<Integer> findIndexForNextQuality(ListIterator<ChunkProtocolBuffers.Quality> qListIterator) {
        if (qListIterator.hasNext()) {
            ChunkProtocolBuffers.Quality nextQuality = qListIterator.next();
            qListIterator.previous();
            return Optional.of(nextQuality.getPointIndex());
        }
        return Optional.empty();
    }

    private static float getQuality(List<ChunkProtocolBuffers.Quality> qList, ChunkProtocolBuffers.Quality q) {
        float quality;
        if (q.hasVIndex()) {
            quality = qList.get(q.getVIndex()).getV();
        } else {
            quality = q.getV();
        }
        return quality;
    }


    private static double getValue(List<ChunkProtocolBuffers.Point> pList, ChunkProtocolBuffers.Point p) {
        double value;
        if (p.hasVIndex()) {
            value = pList.get(p.getVIndex()).getV();
        } else {
            value = p.getV();
        }
        return value;
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

    /**
     * Gets the time stamp from the point.
     *
     * @param p          the protocol buffers point
     * @return the time stamp of the point or the last offset if the point do not have any information about the time stamp
     */
    private static Optional<Long> getOffsetOfPoint(final ChunkProtocolBuffers.Point p) {
        if (p.hasTint()) return Optional.of((long) p.getTint());
        if (p.hasTlong()) return Optional.of(p.getTlong());
        if (p.hasTintBP()) return Optional.of((long) p.getTintBP());
        if (p.hasTlongBP()) return Optional.of(p.getTlongBP());
        return Optional.empty();
    }

    private static Optional<Long> getOffset(ChunkProtocolBuffers.Point p) {
        return getOffsetOfPoint(p);
    }
}
