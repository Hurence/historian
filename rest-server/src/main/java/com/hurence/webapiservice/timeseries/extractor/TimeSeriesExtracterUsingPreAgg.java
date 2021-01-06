package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;
import com.hurence.webapiservice.http.api.grafana.util.QualityAgg;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.aggs.ChunkAggsCalculator;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TimeSeriesExtracterUsingPreAgg extends AbstractTimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUsingPreAgg.class);

    final ChunkAggsCalculator aggsCalculator;

    final QualityAgg qualityAgg;

    public TimeSeriesExtracterUsingPreAgg(long from, long to, SamplingConf samplingConf, long totalNumberOfPoint, List<AGG> aggregList,
                                          boolean returnQuality, QualityAgg qualityAgg) {
        super(from, to, samplingConf, totalNumberOfPoint, returnQuality);
        aggsCalculator = new ChunkAggsCalculator(aggregList);
        this.qualityAgg = qualityAgg;
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<Chunk> chunks) {
        List<Measure> sampledMeasure = extractPoints(chunks);
        this.sampledMeasures.addAll(sampledMeasure);
        aggsCalculator.updateAggs(chunks);
    }

    @Override
    protected Optional<JsonObject> getAggsAsJson() {
        return aggsCalculator.getAggsAsJson();
    }

    private List<Measure> extractPoints(List<Chunk> chunks) {
        List<List<Chunk>> groupedChunks = groupChunks(chunks, this.samplingConf.getBucketSize());
        return groupedChunks.stream()
                .map(this::sampleChunksIntoOneAggPoint)
                .sorted(Comparator.comparing(Measure::getTimestamp))
                .collect(Collectors.toList());
    }

    private List<List<Chunk>> groupChunks(List<Chunk> chunks, int bucketSize) {
        List<List<Chunk>> groupedChunks = new ArrayList<>();
        int currentPointNumber = 0;
        List<Chunk> bucketOfChunks = new ArrayList<>();
        for (Chunk chunk: chunks) {
            bucketOfChunks.add(chunk);
            currentPointNumber += chunk.getCount();
            if (currentPointNumber >= bucketSize) {
                groupedChunks.add(bucketOfChunks);
                bucketOfChunks = new ArrayList<>();
                currentPointNumber = 0;
            }
        }
        if (!bucketOfChunks.isEmpty())
            groupedChunks.add(bucketOfChunks);
        return groupedChunks;
    }

    private Measure sampleChunksIntoOneAggPoint(List<Chunk> chunks) {
        if (chunks.isEmpty())
            throw new IllegalArgumentException("chunks can not be empty !");
        LOGGER.trace("sampling chunks (showing first one) : {}", chunks.get(0));
        long timestamp = chunks.stream()
                .mapToLong(Chunk::getStart)
                .findFirst()
                .getAsLong();
        double aggValue = getAggValue(chunks);
        Float quality = getQualityValue(chunks);
        if (returnQuality && !quality.isNaN())
            return Measure.fromValueAndQuality(timestamp, aggValue, quality);
        else if (returnQuality)
            return Measure.fromValueAndQuality(timestamp, aggValue, Measure.DEFAULT_QUALITY);
        else
            return Measure.fromValue(timestamp, aggValue);
    }

    private double getAggValue(List<Chunk> chunks) {
        double aggValue;
        switch (samplingConf.getAlgo()) {
            case AVERAGE:
                double sum = chunks.stream()
                        .mapToDouble(Chunk::getSum)
                        .sum();
                long numberOfPoint = chunks.stream()
                        .mapToLong(Chunk::getCount)
                        .sum();
                aggValue = BigDecimal.valueOf(sum)
                        .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                        .doubleValue();
                break;
            case FIRST:
                aggValue = chunks.stream()
                        .mapToDouble(Chunk::getFirst)
                        .findFirst()
                        .getAsDouble();
                break;
            case MIN:
                aggValue = chunks.stream()
                        .mapToDouble(Chunk::getMin)
                        .min()
                        .getAsDouble();
                break;
            case MAX:
                aggValue = chunks.stream()
                        .mapToDouble(Chunk::getMax)
                        .max()
                        .getAsDouble();
                break;
            case MODE_MEDIAN:
            case LTTB:
            case MIN_MAX:
            case NONE:
            default:
                throw new IllegalStateException("Unsupported algo: " + samplingConf.getAlgo());
        }
        return aggValue;
    }

    private Float getQualityValue(List<Chunk> chunks) {
        Float quality;
        switch (qualityAgg) {
            case AVG:
            case NONE:  // is it good to chose AVG if qualityAgg is NONE ?
                try {
                    long numberOfPoint = chunks.stream()
                            .mapToLong(Chunk::getCount)
                            .sum();
                    float qualitySum = (float) chunks.stream()
                            .mapToDouble(Chunk::getQualitySum)
                            .sum();
                    quality = BigDecimal.valueOf(qualitySum)
                            .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                            .floatValue();
                } catch (Exception e) {
                    quality = Float.NaN;
                }
                break;
            case FIRST:
                try {
                    quality = (float) chunks.stream()
                            .mapToDouble(Chunk::getQualityFirst)
                            .findFirst()
                            .getAsDouble();
                } catch (Exception e) {
                    quality = Float.NaN;
                }
                break;
            case MIN:
                try {
                    quality = (float) chunks.stream()
                            .mapToDouble(Chunk::getQualityMin)
                            .min()
                            .getAsDouble();
                } catch (Exception e) {
                    quality = Float.NaN;
                }
                break;
            case MAX:
                try {
                    quality = (float) chunks.stream()
                            .mapToDouble(Chunk::getQualityMax)
                            .max()
                            .getAsDouble();
                } catch (Exception e) {
                    quality = Float.NaN;
                }
                break;
            /*case NONE:
                quality = Float.NaN;
                break;*/
            default:
                throw new IllegalStateException("Unsupported algo: " + qualityAgg);
        }
        return quality;
    }

}
