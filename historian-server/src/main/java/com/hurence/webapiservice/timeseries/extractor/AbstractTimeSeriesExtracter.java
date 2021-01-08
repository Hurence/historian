package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
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

import static com.hurence.timeseries.model.Measure.DEFAULT_QUALITY;

public abstract class AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractTimeSeriesExtracter.class);

    final long from;
    final long to;
    final SamplingConf samplingConf;
    protected final List<Chunk> chunks = new ArrayList<>();
    final List<Measure> sampledMeasures = new ArrayList<>();

    private long totalChunkCounter = 0L;
    long totalPointCounter = 0L;
    long pointCounter = 0L;
    boolean returnQuality;

    public AbstractTimeSeriesExtracter(long from, long to,
                                       SamplingConf samplingConf,
                                       long totalNumberOfPoint,
                                       boolean returnQuality) {
        this.from = from;
        this.to = to;
        this.samplingConf = TimeSeriesExtracterUtil.calculSamplingConf(samplingConf, totalNumberOfPoint);
        this.returnQuality = returnQuality;
        LOGGER.debug("Initialized {}  with samplingConf : {}", this.getClass(), this.samplingConf);
    }

    @Override
    public void addChunk(Chunk chunk) {
        totalChunkCounter++;
        pointCounter+=chunk.getCount();
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
     * Extract/Sample measures from the list of chunks in buffer using a strategy. Add them into sampled measures then reset buffer
     */
    protected void samplePointsInBufferAndCalculAggregThenReset() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("sample measures in buffer has been called with chunks : {}",
                    chunks.stream().map(Chunk::toString).collect(Collectors.joining("\n")));
        }
        samplePointsFromChunksAndCalculAggreg(from, to, chunks);
        chunks.clear();
        totalPointCounter +=pointCounter;
        pointCounter = 0;
    }

    /**
     * Sample measures from the list of chunks using a strategy. Add them into sampled measures then reset buffer
     */
    protected abstract void samplePointsFromChunksAndCalculAggreg(long from, long to, List<Chunk> chunks);

    protected abstract Optional<JsonObject> getAggsAsJson();

    @Override
    public JsonObject getTimeSeries() {
        List<JsonArray> points = sampledMeasures.stream()
                /*
                * Here we have to sort sampled measures in case some chunks are intersecting.
                * The best would be to repare the chunk though. A mechanism that would track those chunks and rebuild them
                * may be the best solution I think. The requesting code here should suppose chunks are not intersecting.
                * We sort just so that user can not realize there is a problem in chunks.
                */
                .sorted(Comparator.comparing(Measure::getTimestamp).thenComparing(Measure::getValue))
                .map(this::returnPoint)
                .collect(Collectors.toList());
        JsonObject toReturn = new JsonObject();
        if (!points.isEmpty())
        {
            toReturn.put(TIMESERIE_POINT, new JsonArray(points))
                    .put(TOTAL_POINTS, points.size());
            getAggsAsJson()
                    .ifPresent(aggs -> toReturn.put(TIMESERIE_AGGS, aggs));
            LOGGER.trace("getTimeSeries return : {}", toReturn.encodePrettily());
        }
        return toReturn;
    }

    private JsonArray returnPoint(Measure measure) {
        if (returnQuality && measure.hasQuality())
            return new JsonArray().add(measure.getValue()).add(measure.getTimestamp()).add(measure.getQuality());
        else if (returnQuality && !measure.hasQuality())
            return new JsonArray().add(measure.getValue()).add(measure.getTimestamp()).add(DEFAULT_QUALITY);
        else
            return new JsonArray().add(measure.getValue()).add(measure.getTimestamp());
    }

    @Override
    public long chunkCount() {
        return totalChunkCounter;
    }

    @Override
    public long pointCount() {
        return totalPointCounter;
    }
}
