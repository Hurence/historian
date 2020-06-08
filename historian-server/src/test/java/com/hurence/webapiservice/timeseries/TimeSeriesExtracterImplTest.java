package com.hurence.webapiservice.timeseries;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static com.hurence.webapiservice.modele.AGG.*;
import static com.hurence.webapiservice.timeseries.TimeSeriesExtracter.*;

public class TimeSeriesExtracterImplTest {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImplTest.class);

    private long START_CHUNK_1 = 1477895624866L;
    private long MIDDLE_CHUNK_1 = 1477895624867L;
    private long END_CHUNK_1 = 1477895624868L;
    private long START_CHUNK_2 = 1477895624869L;
    private long MIDDLE_CHUNK_2 = 1477895624870L;
    private long END_CHUNK_2 = 1477895624871L;

    JsonObject getChunk1() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new Point(0, START_CHUNK_1, 1),
                new Point(0, MIDDLE_CHUNK_1, 2),
                new Point(0, END_CHUNK_1, 3)
        ));
        return chunk.toJson("id1");
    }


    JsonObject getChunk2() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new Point(0, START_CHUNK_2, 4),
                new Point(0, MIDDLE_CHUNK_2, 5),
                new Point(0, END_CHUNK_2, 6)
        ));
        return chunk.toJson("id1");
    }

    JsonObject getConflictingChunk() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new Point(0, MIDDLE_CHUNK_1, 4),
                new Point(0, START_CHUNK_2, 5),
                new Point(0, MIDDLE_CHUNK_2, 6)
        ));
        return chunk.toJson("id1");
    }

    @Test
    public void testNoSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3),
                3, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, MIDDLE_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.0, END_CHUNK_1)));
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 6.0);
        aggregation.put(MAX, 3.0);
        aggregation.put(COUNT, 3);
        aggregation.put(AVG, 2.0);
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_NAME, "fake")
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerPartOfOneChunk() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , MIDDLE_CHUNK_1,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3),
                3, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());//TODO bug should be 2
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, MIDDLE_CHUNK_1)));
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 3.0);
        aggregation.put(MAX, 2.0);
        aggregation.put(COUNT, 2);
        aggregation.put(AVG, 1.5);
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_NAME, "fake")
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , END_CHUNK_1,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3),
                3, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.5, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.0, END_CHUNK_1)));
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 6.0);
        aggregation.put(MAX, 3.0);
        aggregation.put(COUNT, 3);
        aggregation.put(AVG, 2.0);
        Assert.assertEquals(new JsonObject()
                .put(TIMESERIE_NAME, "fake")
                .put(TIMESERIE_POINT, expectedPoints)
                .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3),
                6, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.flush();
        Assert.assertEquals(2, extractor.chunkCount());
        Assert.assertEquals(6, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.5, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.5, END_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(5.5, MIDDLE_CHUNK_2)));
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 21.0);
        aggregation.put(MAX, 6.0);
        aggregation.put(COUNT, 6);
        aggregation.put(AVG, 3.5);
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_NAME, "fake")
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerWithIntersectingChunks() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 9),
                9, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.addChunk(getConflictingChunk());
        extractor.flush();
        Assert.assertEquals(3, extractor.chunkCount());
        Assert.assertEquals(9, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, MIDDLE_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(4.0, MIDDLE_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.0, END_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(4.0, START_CHUNK_2)));
        expectedPoints.add(new JsonArray(Arrays.asList(5.0, START_CHUNK_2)));
        expectedPoints.add(new JsonArray(Arrays.asList(5.0, 1477895624870L)));
        expectedPoints.add(new JsonArray(Arrays.asList(6.0, 1477895624870L)));
        expectedPoints.add(new JsonArray(Arrays.asList(6.0, END_CHUNK_2)));
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 36.0);
        aggregation.put(MAX, 6.0);
        aggregation.put(COUNT, 9);
        aggregation.put(AVG, 4.0);
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_NAME, "fake")
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerWithIntersectingChunks2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 2),
                9, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.addChunk(getConflictingChunk());
        extractor.flush();
        Assert.assertEquals(3, extractor.chunkCount());
        Assert.assertEquals(9, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(2.8, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(5.5, 1477895624869L)));
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 36.0);
        aggregation.put(MAX, 6.0);
        aggregation.put(COUNT, 9);
        aggregation.put(AVG, 4.0);
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_NAME, "fake")
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }


    @Test
    public void testAggsWithSeveralFlush() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3),
                9, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.flush();
        extractor.addChunk(getChunk2());
        extractor.flush();
        extractor.addChunk(getConflictingChunk());
        extractor.flush();
        Assert.assertEquals(3, extractor.chunkCount());
        Assert.assertEquals(9, extractor.pointCount());
        HashMap<AGG, Number> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(SUM, 36.0);
        aggregation.put(MAX, 6.0);
        aggregation.put(COUNT, 9.0);
        aggregation.put(AVG, 4.0);
        JsonObject result = extractor.getTimeSeries();
        Assert.assertEquals(new JsonObject()
                .put(TIMESERIE_AGGS, aggregation)
                .getJsonObject(TIMESERIE_AGGS),
                result.getJsonObject(TIMESERIE_AGGS));
    }
}
