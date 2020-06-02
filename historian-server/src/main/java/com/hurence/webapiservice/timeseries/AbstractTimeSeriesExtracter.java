package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public abstract class AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractTimeSeriesExtracter.class);

    final long from;
    final long to;
    final SamplingConf samplingConf;
    private final String metricName;
    protected final List<JsonObject> chunks = new ArrayList<>();
    final List<Point> sampledPoints = new ArrayList<>();
    List<String> aggregList = new ArrayList<>();
    final Map<String, Double> aggregValuesList = new HashMap<>();
    private long totalChunkCounter = 0L;
    long toatlPointCounter = 0L;
    long pointCounter = 0L;

    public AbstractTimeSeriesExtracter(String metricName, long from, long to,
                                       SamplingConf samplingConf,
                                       long totalNumberOfPoint, List<String> aggregList) {
        this.metricName = metricName;
        this.from = from;
        this.to = to;
        this.samplingConf = TimeSeriesExtracterUtil.calculSamplingConf(samplingConf, totalNumberOfPoint);
        this.aggregList = aggregList;
        LOGGER.debug("Initialized {}  with samplingConf : {}", this.getClass(), this.samplingConf);
    }

    @Override
    public void addChunk(JsonObject chunk) {
        totalChunkCounter++;
        pointCounter+=chunk.getLong(HistorianFields.RESPONSE_CHUNK_COUNT_FIELD);
        chunks.add(chunk);
        if (isBufferFull()) {
            calculateAggreg(); // is this correct
            samplePointsInBufferThenReset();
        }
    }

    public boolean isBufferFull() {
        return pointCounter > 4000 && pointCounter >= samplingConf.getBucketSize();
    }

    @Override
    public void flush() {
        if (!chunks.isEmpty())
            samplePointsInBufferThenReset();
    }

    /**
     * Calcule the aggregations from the list of aggregations and the Lst of chunks . Add them into aggreg values list.
     */
    public void calculateAggreg() {
        aggregList.stream().map(AGG::valueOf).forEach(agg -> {
            double aggValue;
            switch (agg) {
                case AVG:
                    double avg = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_AVG_FIELD))
                            .sum();
                    long numberOfPoint = chunks.stream()
                            .mapToLong(chunk -> chunk.getLong(RESPONSE_CHUNK_COUNT_FIELD))
                            .sum();
                    aggValue = BigDecimal.valueOf(avg)
                            .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                            .doubleValue();
                    aggregValuesList.put("AVG", aggValue);
                    break;
                case SUM:
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                            .sum();
                    aggregValuesList.put("SUM", aggValue);
                    break;
                case MIN:
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD))
                            .min()
                            .getAsDouble();
                    aggregValuesList.put("MIN", aggValue);
                    break;
                case MAX:
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MAX_FIELD))
                            .max()
                            .getAsDouble();
                    aggregValuesList.put("MAX", aggValue);
                    break;
                case COUNT:
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_COUNT_FIELD))
                            .count();
                    aggregValuesList.put("COUNT", aggValue);
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
    }

    /**
     * Extract/Sample points from the list of chunks in buffer using a strategy. Add them into sampled points then reset buffer
     */
    protected void samplePointsInBufferThenReset() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("sample points in buffer has been called with chunks : {}",
                    chunks.stream().map(JsonObject::encodePrettily).collect(Collectors.joining("\n")));
        }
        samplePointsFromChunks(from, to, chunks);
        chunks.clear();
        toatlPointCounter+=pointCounter;
        pointCounter = 0;
    }

    /**
     * Sample points from the list of chunks using a strategy. Add them into sampled points then reset buffer
     */
    protected abstract void samplePointsFromChunks(long from, long to, List<JsonObject> chunks);

    @Override
    public JsonObject getTimeSeries() {
        List<JsonArray> points = sampledPoints.stream()
                /*
                * Here we have to sort sampled points in case some chunks are intersecting.
                * The best would be to repare the chunk though. A mechanism that would track those chunks and rebuild them
                * may be the best solution I think. The requesting code here should suppose chunks are not intersecting.
                * We sort just so that user can not realize there is a problem in chunks.
                */
                .sorted(Comparator.comparing(Point::getTimestamp))
                .map(p -> new JsonArray().add(p.getValue()).add(p.getTimestamp()))
                .collect(Collectors.toList());
        JsonObject toReturn = new JsonObject()
                .put(TIMESERIE_NAME, metricName)
                .put(TIMESERIE_POINT, new JsonArray(points));
        if (!aggregValuesList.isEmpty()) {
            toReturn.put(AGGREGATION, aggregValuesList);
        }
        LOGGER.trace("getTimeSeries return : {}", toReturn.encodePrettily());
        return toReturn;
    }

    @Override
    public long chunkCount() {
        return totalChunkCounter;
    }

    @Override
    public long pointCount() {
        return toatlPointCounter;
    }
}
