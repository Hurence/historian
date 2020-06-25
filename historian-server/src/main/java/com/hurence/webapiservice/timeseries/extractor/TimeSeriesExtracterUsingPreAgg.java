package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.logisland.record.Point;
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

import static com.hurence.historian.modele.HistorianFields.*;

public class TimeSeriesExtracterUsingPreAgg extends AbstractTimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUsingPreAgg.class);

    final ChunkAggsCalculator aggsCalculator;

    public TimeSeriesExtracterUsingPreAgg(long from, long to, SamplingConf samplingConf, long totalNumberOfPoint, List<AGG> aggregList) {
        super(from, to, samplingConf, totalNumberOfPoint);
        aggsCalculator = new ChunkAggsCalculator(aggregList);
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks) {
        List<Point> sampledPoint = extractPoints(chunks);
        this.sampledPoints.addAll(sampledPoint);
        aggsCalculator.updateAggs(chunks);
    }

    @Override
    protected Optional<JsonObject> getAggsAsJson() {
        return aggsCalculator.getAggsAsJson();
    }

    private List<Point> extractPoints(List<JsonObject> chunks) {
        List<List<JsonObject>> groupedChunks = groupChunks(chunks, this.samplingConf.getBucketSize());
        return groupedChunks.stream()
                .map(this::sampleChunksIntoOneAggPoint)
                .sorted(Comparator.comparing(Point::getTimestamp))
                .collect(Collectors.toList());
    }

    private List<List<JsonObject>> groupChunks(List<JsonObject> chunks, int bucketSize) {
        List<List<JsonObject>> groupedChunks = new ArrayList<>();
        int currentPointNumber = 0;
        List<JsonObject> bucketOfChunks = new ArrayList<>();
        for (JsonObject chunk: chunks) {
            bucketOfChunks.add(chunk);
            currentPointNumber += chunk.getInteger(CHUNK_COUNT_FIELD);
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

    private Point sampleChunksIntoOneAggPoint(List<JsonObject> chunks) {
        if (chunks.isEmpty())
            throw new IllegalArgumentException("chunks can not be empty !");
        LOGGER.trace("sampling chunks (showing first one) : {}", chunks.get(0).encodePrettily());
        long timestamp = chunks.stream()
                .mapToLong(chunk -> chunk.getLong(CHUNK_START_FIELD))
                .findFirst()
                .getAsLong();
        double aggValue;
        switch (samplingConf.getAlgo()) {
            case AVERAGE:
                double sum = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(CHUNK_SUM_FIELD))
                        .sum();
                long numberOfPoint = chunks.stream()
                        .mapToLong(chunk -> chunk.getLong(CHUNK_COUNT_FIELD))
                        .sum();
                aggValue = BigDecimal.valueOf(sum)
                        .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                        .doubleValue();
                break;
            case FIRST:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(CHUNK_FIRST_VALUE_FIELD))
                        .findFirst()
                        .getAsDouble();
                break;
            case MIN:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(CHUNK_MIN_FIELD))
                        .min()
                        .getAsDouble();
                break;
            case MAX:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(CHUNK_MAX_FIELD))
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
        return new Point(0, timestamp, aggValue);
    }
}
