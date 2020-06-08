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

import static com.hurence.webapiservice.modele.AGG.MAX;
import static com.hurence.webapiservice.modele.AGG.MIN;

public class TimeSeriesExtracterImplTest {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImplTest.class);


    JsonObject getChunk1() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new Point(0, 1477895624866L, 1),
                new Point(0, 1477916224866L, 2),
                new Point(0, 1477917224866L, 3)
        ));
        return chunk.toJson("id1");
    }


    JsonObject getChunk2() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new Point(0, 1477916224866L, 4),
                new Point(0, 1477916224867L, 5),
                new Point(0, 1477916224868L, 6)
        ));
        return chunk.toJson("id1");
    }

    @Test
    public void testNoSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3), 3, Arrays.asList(MAX, MIN));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, 1477916224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.0, 1477917224866L)));
        HashMap<AGG, Double> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(MAX, 3.0);
        Assert.assertEquals(new JsonObject()
                        .put("name", "fake")
                        .put("datapoints", expectedPoints)
                        .put("aggregation", aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerPartOfOneChunk() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624865L , 1477916224867L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3), 3, Arrays.asList(MAX, MIN));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, 1477916224866L)));
        HashMap<AGG, Double> aggregation = new HashMap<>();
        aggregation.put(MIN, 1.0);
        aggregation.put(MAX, 2.0);
        Assert.assertEquals(new JsonObject()
                        .put("name", "fake")
                        .put("datapoints", expectedPoints)
                        .put("aggregation", aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3), 3, Arrays.asList(MAX, MIN));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(3, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.5, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.0, 1477917224866L)));
        HashMap<AGG, Double> aggregation = new HashMap<>();
        aggregation.put(MAX, 3.0);
        aggregation.put(MIN, 1.0);
        Assert.assertEquals(new JsonObject()
                .put("name", "fake")
                .put("datapoints", expectedPoints)
                .put("aggregation", aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerWithIntersectingChunks() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 6), 6, Arrays.asList(MAX, MIN));
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.flush();
        Assert.assertEquals(2, extractor.chunkCount());
        Assert.assertEquals(6, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, 1477916224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(4.0, 1477916224866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(5.0, 1477916224867L)));
        expectedPoints.add(new JsonArray(Arrays.asList(6.0, 1477916224868L)));
        expectedPoints.add(new JsonArray(Arrays.asList(3.0, 1477917224866L)));
        HashMap<AGG, Double> aggregation = new HashMap<>();
        aggregation.put(MAX, 6.0);
        aggregation.put(MIN, 1.0);
        Assert.assertEquals(new JsonObject()
                        .put("name", "fake")
                        .put("datapoints", expectedPoints)
                        .put("aggregation", aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testNoSamplerWithIntersectingChunks2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl("fake",
                1477895624866L , 1477917224866L,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 2), 6, Arrays.asList(MAX, MIN));
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.flush();
        Assert.assertEquals(2, extractor.chunkCount());
        Assert.assertEquals(6, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1, 1477895624866L)));
        expectedPoints.add(new JsonArray(Arrays.asList(5, 1477916224867L)));
        HashMap<AGG, Double> aggregation = new HashMap<>();
        aggregation.put(MAX, 6.0);
        aggregation.put(MIN, 1.0);
        Assert.assertEquals(new JsonObject()
                        .put("name", "fake")
                        .put("datapoints", expectedPoints)
                        .put("aggregation", aggregation)
                , extractor.getTimeSeries());
    }

}
