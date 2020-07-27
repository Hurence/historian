package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.sampling.Sampler;
import com.hurence.timeseries.sampling.SamplerFactory;
import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.webapiservice.historian.util.ChunkUtil;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.util.BucketUtils;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.historian.modele.HistorianFields.*;


public class TimeSeriesExtracterUtil {
    public final static String TIMESERIES_TIMESTAMPS = "timestamps";
    public final static String TIMESERIES_VALUES = "values";
    public final static SamplingAlgorithm DEFAULT_SAMPLING_ALGORITHM = SamplingAlgorithm.AVERAGE;
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUtil.class);


    private TimeSeriesExtracterUtil() {}
    /**
     *
     * @param from
     * @param to
     * @param chunks
     * @return return all points uncompressing chunks
     */
    public static List<PointImpl> extractPoints(long from, long to, List<JsonObject> chunks) {
        return extractPointsAsStream(from, to, chunks).collect(Collectors.toList());
    }

    public static Stream<PointImpl> extractPointsAsStream(long from, long to, List<JsonObject> chunks) {
        return chunks.stream()
                .flatMap(chunk -> {
                    byte[] binaryChunk = chunk.getBinary(CHUNK_VALUE_FIELD);
                    long chunkStart = chunk.getLong(CHUNK_START_FIELD);
                    long chunkEnd = chunk.getLong(CHUNK_END_FIELD);
                    try {
                        return BinaryCompactionUtil.unCompressPoints(binaryChunk, chunkStart, chunkEnd, from, to).stream();
                    } catch (IOException ex) {
                        throw new IllegalArgumentException("error during uncompression of a chunk !", ex);
                    }
                });
    }

    /**
     *
     * @param samplingConf how to sample points to retrieve
     * @param chunks to sample, chunks should be corresponding to the same timeserie !*
     *               Should contain the compressed binary points as well as all needed aggs.
     *               Chunks should be ordered as well.
     * @return sampled points as an array
     * <pre>
     * {
     *     {@value TIMESERIES_TIMESTAMPS} : [longs]
     *     {@value TIMESERIES_VALUES} : [doubles]
     * }
     * DOCS contains at minimum chunk_value, chunk_start
     * </pre>
     */
    public static List<PointImpl> extractPointsThenSample(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(samplingConf.getAlgo(), samplingConf.getBucketSize());
        return sampler.sample(extractPoints(from, to, chunks));
    }

    /**
     *
     * @param samplingConf how to sample points to retrieve
     * @param chunks to sample, chunks should be corresponding to the same timeserie !*
     *               Should contain the compressed binary points as well as all needed aggs.
     *               Chunks should be ordered as well.
     * @return sampled points as an array
     * <pre>
     * {
     *     {@value TIMESERIES_TIMESTAMPS} : [longs]
     *     {@value TIMESERIES_VALUES} : [doubles]
     * }
     * DOCS contains at minimum chunk_value, chunk_start
     * </pre>
     */
    public static List<PointImpl> extractPointsThenSortThenSample(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Stream<PointImpl> extractedPoints = extractPointsAsStream(from, to, chunks);
        Stream<PointImpl> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(PointImpl::getTimestamp));
        return samplePoints(samplingConf, chunks, sortedPoints);
    }

    public static List<PointImpl> samplePoints(SamplingConf samplingConf, List<JsonObject> chunks, Stream<PointImpl> sortedPoints) {
        int totalNumberOfPoint = ChunkUtil.countTotalNumberOfPointInChunks(chunks);
        Sampler<PointImpl> sampler = getPointSampler(samplingConf, totalNumberOfPoint);
        return sampler.sample(sortedPoints.collect(Collectors.toList()));
    }

    public static Sampler<PointImpl> getPointSampler(SamplingConf samplingConf, long totalNumberOfPoint) {
        SamplingConf calculatedConf = calculSamplingConf(samplingConf, totalNumberOfPoint);
        return SamplerFactory.getPointSampler(calculatedConf.getAlgo(), calculatedConf.getBucketSize());
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


    private static int calculBucketSize(int maxPoint, int totalNumberOfPoint) {
        return BucketUtils.calculBucketSize(totalNumberOfPoint, maxPoint);
    }

    private static int calculBucketSize(int maxPoint, long totalNumberOfPoint) {
        return BucketUtils.calculBucketSize(totalNumberOfPoint, maxPoint);
    }


    public static JsonObject formatTimeSeriePointsJson(List<PointImpl> sampledPoints) {
        List<Long> timestamps = sampledPoints.stream()
                .map(PointImpl::getTimestamp)
                .collect(Collectors.toList());
        List<Double> values = sampledPoints.stream()
                .map(PointImpl::getValue)
                .collect(Collectors.toList());
        return new JsonObject()
                .put(TIMESERIES_TIMESTAMPS, timestamps)
                .put(TIMESERIES_VALUES, values);
    }
}
