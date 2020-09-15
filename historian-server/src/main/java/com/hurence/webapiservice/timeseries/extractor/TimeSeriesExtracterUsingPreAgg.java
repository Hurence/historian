package com.hurence.webapiservice.timeseries.extractor;


import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;
import com.hurence.timeseries.modele.points.PointImpl;
import com.hurence.timeseries.modele.points.PointWithQualityImpl;
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
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<ChunkVersionCurrent> chunks) {
        List<Point> sampledPoint = extractPoints(chunks);
        this.sampledPoints.addAll(sampledPoint);
        aggsCalculator.updateAggs(chunks);
    }

    @Override
    protected Optional<JsonObject> getAggsAsJson() {
        return aggsCalculator.getAggsAsJson();
    }

    private List<Point> extractPoints(List<ChunkVersionCurrent> chunks) {
        List<List<ChunkVersionCurrent>> groupedChunks = groupChunks(chunks, this.samplingConf.getBucketSize());
        return groupedChunks.stream()
                .map(this::sampleChunksIntoOneAggPoint)
                .sorted(Comparator.comparing(Point::getTimestamp))
                .collect(Collectors.toList());
    }

    private List<List<ChunkVersionCurrent>> groupChunks(List<ChunkVersionCurrent> chunks, int bucketSize) {
        List<List<ChunkVersionCurrent>> groupedChunks = new ArrayList<>();
        int currentPointNumber = 0;
        List<ChunkVersionCurrent> bucketOfChunks = new ArrayList<>();
        for (ChunkVersionCurrent chunk: chunks) {
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

    private Point sampleChunksIntoOneAggPoint(List<ChunkVersionCurrent> chunks) {
        if (chunks.isEmpty())
            throw new IllegalArgumentException("chunks can not be empty !");
        LOGGER.trace("sampling chunks (showing first one) : {}", chunks.get(0));
        long timestamp = chunks.stream()
                .mapToLong(ChunkVersionCurrent::getStart)
                .findFirst()
                .getAsLong();
        double aggValue = getAggValue(chunks);
        Float quality = getQualityValue(chunks);
        if (returnQuality && !quality.isNaN())
            return new PointWithQualityImpl(timestamp, aggValue, quality);
        else if (returnQuality)
            return new PointWithQualityImpl(timestamp, aggValue, Point.DEFAULT_QUALITY);
        else
            return new PointImpl(timestamp, aggValue);
    }

    private double getAggValue(List<ChunkVersionCurrent> chunks) {
        double aggValue;
        switch (samplingConf.getAlgo()) {
            case AVERAGE:
                double sum = chunks.stream()
                        .mapToDouble(ChunkVersionCurrent::getSum)
                        .sum();
                long numberOfPoint = chunks.stream()
                        .mapToLong(ChunkVersionCurrent::getCount)
                        .sum();
                aggValue = BigDecimal.valueOf(sum)
                        .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                        .doubleValue();
                break;
            case FIRST:
                aggValue = chunks.stream()
                        .mapToDouble(ChunkVersionCurrent::getFirst)
                        .findFirst()
                        .getAsDouble();
                break;
            case MIN:
                aggValue = chunks.stream()
                        .mapToDouble(ChunkVersionCurrent::getMin)
                        .min()
                        .getAsDouble();
                break;
            case MAX:
                aggValue = chunks.stream()
                        .mapToDouble(ChunkVersionCurrent::getMax)
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
    private Float getQualityValue(List<ChunkVersionCurrent> chunks) {
        Float quality;
        switch (qualityAgg) {
            case AVG:
            case NONE:  // is it good to chose AVG if qualityAgg is NONE ?
                try {
                    long numberOfPoint = chunks.stream()
                            .mapToLong(ChunkVersionCurrent::getCount)
                            .sum();
                    float qualitySum = (float) chunks.stream()
                            .mapToDouble(ChunkVersionCurrent::getQualitySum)
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
                            .mapToDouble(ChunkVersionCurrent::getQualityFirst)
                            .findFirst()
                            .getAsDouble();
                } catch (Exception e) {
                    quality = Float.NaN;
                }
                break;
            case MIN:
                try {
                    quality = (float) chunks.stream()
                            .mapToDouble(ChunkVersionCurrent::getQualityMin)
                            .min()
                            .getAsDouble();
                } catch (Exception e) {
                    quality = Float.NaN;
                }
                break;
            case MAX:
                try {
                    quality = (float) chunks.stream()
                            .mapToDouble(ChunkVersionCurrent::getQualityMax)
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
