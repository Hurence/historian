package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.timeseries.modele.Point;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractTimeSeriesExtracter.class);

    final long from;
    final long to;
    final SamplingConf samplingConf;
    protected final List<JsonObject> chunks = new ArrayList<>();
    final List<Point> sampledPoints = new ArrayList<>();
    private long totalChunkCounter = 0L;
    long toatlPointCounter = 0L;
    long pointCounter = 0L;

    public AbstractTimeSeriesExtracter(long from, long to,
                                       SamplingConf samplingConf,
                                       long totalNumberOfPoint) {
        this.from = from;
        this.to = to;
        this.samplingConf = TimeSeriesExtracterUtil.calculSamplingConf(samplingConf, totalNumberOfPoint);
        LOGGER.debug("Initialized {}  with samplingConf : {}", this.getClass(), this.samplingConf);
    }

    @Override
    public void addChunk(JsonObject chunk) {
        totalChunkCounter++;
        pointCounter+=chunk.getLong(HistorianFields.CHUNK_COUNT_FIELD);
        chunks.add(chunk);
        if (isBufferFull()) {
            samplePointsInBufferAndCalculAggregThenReset();
        }
    }

    public boolean isBufferFull() {
        return pointCounter > 4000 && pointCounter >= samplingConf.getBucketSize();
    }

    @Override
    public void flush() {
        if (!chunks.isEmpty())
            samplePointsInBufferAndCalculAggregThenReset();
    }


    /**
     * Extract/Sample points from the list of chunks in buffer using a strategy. Add them into sampled points then reset buffer
     */
    protected void samplePointsInBufferAndCalculAggregThenReset() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("sample points in buffer has been called with chunks : {}",
                    chunks.stream().map(JsonObject::encodePrettily).collect(Collectors.joining("\n")));
        }
        samplePointsFromChunksAndCalculAggreg(from, to, chunks);
        chunks.clear();
        toatlPointCounter+=pointCounter;
        pointCounter = 0;
    }

    /**
     * Sample points from the list of chunks using a strategy. Add them into sampled points then reset buffer
     */
    protected abstract void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks);

    protected abstract Optional<JsonObject> getAggsAsJson();

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
                .put(TIMESERIE_POINT, new JsonArray(points));
        getAggsAsJson()
                .ifPresent(aggs -> toReturn.put(TIMESERIE_AGGS, aggs));
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
