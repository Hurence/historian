package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.historian.mymodele.Chunk;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.util.BucketUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;


public class TimeSeriesExtracterUtil {

    public final static SamplingAlgorithm DEFAULT_SAMPLING_ALGORITHM = SamplingAlgorithm.AVERAGE;


    private TimeSeriesExtracterUtil() {}

    public static Stream<PointImpl> extractPointsAsStream(long from, long to, List<Chunk> chunks) {
        return chunks.stream()
                .flatMap(chunk -> {
                    byte[] binaryChunk = chunk.getValueAsBinary();
                    long chunkStart = chunk.getStart();
                    long chunkEnd = chunk.getEnd();
                    try {
                        return BinaryCompactionUtil.unCompressPoints(binaryChunk, chunkStart, chunkEnd, from, to).stream();
                    } catch (IOException ex) {
                        throw new IllegalArgumentException("error during uncompression of a chunk !", ex);
                    }
                });
    }

    /**
     * return the sampling in input if it is compatible with totalNumberOfPoint to sample.
     * If totalNumberOfPoint > maxpoint then we have to sample.
     * Calcul bucket size if needed (dependending on samplingConf.getMaxPoint() and totalNumberOfPoint
     * @param samplingConf
     * @param totalNumberOfPoint
     * @return
     */
    public static SamplingConf calculSamplingConf(SamplingConf samplingConf, long totalNumberOfPoint) {
        SamplingAlgorithm algorithm = calculSamplingAlgorithm(samplingConf, totalNumberOfPoint);
        int bucketSize = samplingConf.getBucketSize();
        if (bucketSize == 0) throw new IllegalArgumentException("bucket size can not be '0' !");
        long numberOfPointToReturnWithCurrentBucket = totalNumberOfPoint / bucketSize;
        if (totalNumberOfPoint > samplingConf.getMaxPoint() &&
                numberOfPointToReturnWithCurrentBucket > samplingConf.getMaxPoint()) {
            //verify there is not too many point to return them all or to return them with user chosen bucket size
            // otherwise recalcul bucket size accordingly.
            bucketSize = calculBucketSize(samplingConf.getMaxPoint(), totalNumberOfPoint);
        }
        return new SamplingConf(algorithm, bucketSize, samplingConf.getMaxPoint());
    }

    public static SamplingAlgorithm calculSamplingAlgorithm(SamplingConf samplingConf, long totalNumberOfPoint) {
        if (samplingConf.getAlgo() == SamplingAlgorithm.NONE && totalNumberOfPoint > samplingConf.getMaxPoint())
            return DEFAULT_SAMPLING_ALGORITHM;
        return samplingConf.getAlgo();
    }

    private static int calculBucketSize(int maxPoint, long totalNumberOfPoint) {
        return BucketUtils.calculBucketSize(totalNumberOfPoint, maxPoint);
    }
}
