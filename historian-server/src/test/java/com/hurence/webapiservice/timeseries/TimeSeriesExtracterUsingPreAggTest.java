package com.hurence.webapiservice.timeseries;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracter;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterImpl;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterUsingPreAgg;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.hurence.webapiservice.modele.AGG.*;
import static com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracter.TIMESERIE_AGGS;
import static com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracter.TIMESERIE_POINT;

public class TimeSeriesExtracterUsingPreAggTest {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUsingPreAggTest.class);


    private long START_CHUNK_2 = 1477917224866L;
    private long START_CHUNK_1 = 1477895624866L;
    JsonObject getChunk1() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new PointImpl(START_CHUNK_1, 1),
                new PointImpl(1477916224866L, 1),
                new PointImpl(1477916224867L, 1),
                new PointImpl(1477916224868L, 1),
                new PointImpl(1477916224869L, 1),
                new PointImpl(1477916224870L, 1),
                new PointImpl(1477916224871L, 1),
                new PointImpl(1477916224872L, 1),
                new PointImpl(1477917224865L, 1)
        ));
        return chunk.toJson("id1");
    }

    JsonObject getChunk2() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new PointImpl( START_CHUNK_2, 2),
                new PointImpl( 1477917224867L, 2),
                new PointImpl( 1477917224868L, 2)
        ));
        return chunk.toJson("id2");
    }


    JsonObject getChunk3() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new PointImpl(1477917224868L, 3),
                new PointImpl(1477917224869L, 3),
                new PointImpl(1477917224870L, 3)
        ));
        return chunk.toJson("id2");
    }

    JsonObject getChunk4() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new PointImpl( 1477917224870L, 4),
                new PointImpl( 1477917224871L, 4),
                new PointImpl( 1477917224872L, 4)
        ));
        return chunk.toJson("id2");
    }

    JsonObject getChunk5() {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints("fake", Arrays.asList(
                new PointImpl( 1477917224873L, 5),
                new PointImpl( 1477917224874L, 5),
                new PointImpl( 1477917224875L, 5)
        ));
        return chunk.toJson("id2");
    }

    @Test
    public void testNoSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg(
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.NONE, 2, 3),
                9, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.flush();
        Assert.assertEquals(1, extractor.chunkCount());
        Assert.assertEquals(9, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, START_CHUNK_1)));

        JsonObject aggregation = new JsonObject();
        aggregation.put(MIN.toString(), 1.0);
        aggregation.put(MAX.toString(), 1.0);
        aggregation.put(SUM.toString(), 9.0);
        aggregation.put(COUNT.toString(), 9.0);
        aggregation.put(AVG.toString(), 1.0);

        Assert.assertEquals(new JsonObject()
                .put(TIMESERIE_POINT, expectedPoints)
                .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }



    @Test
    public void testAvgSampler() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg(
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3),
                15, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.flush();
        Assert.assertEquals(3, extractor.chunkCount());
        Assert.assertEquals(15, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(1.0, START_CHUNK_1)));
        expectedPoints.add(new JsonArray(Arrays.asList(2.5, START_CHUNK_2)));

        JsonObject aggregation = new JsonObject();
        aggregation.put(MIN.toString(), 1.0);
        aggregation.put(MAX.toString(), 3.0);
        aggregation.put(SUM.toString(), 24.0);
        aggregation.put(COUNT.toString(), 15.0);
        aggregation.put(AVG.toString(), 1.6);

        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }

    @Test
    public void testAvgSampler2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg(

                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3),
                12, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.addChunk(getChunk4());
        extractor.addChunk(getChunk5());
        extractor.flush();
        Assert.assertEquals(4, extractor.chunkCount());
        Assert.assertEquals(12, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(2.5, START_CHUNK_2)));
        expectedPoints.add(new JsonArray(Arrays.asList(4.5, 1477917224870L)));
        JsonObject aggregation = new JsonObject();
        aggregation.put(MIN.toString(), 2.0);
        aggregation.put(MAX.toString(), 5.0);
        aggregation.put(SUM.toString(), 42.0);
        aggregation.put(COUNT.toString(), 12.0);
        aggregation.put(AVG.toString(), 3.5);
        Assert.assertEquals(new JsonObject()
                .put(TIMESERIE_POINT, expectedPoints)
                .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }


    @Test
    public void testMinSampler() {
                TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg(
                        Long.MIN_VALUE , Long.MAX_VALUE,
                                new SamplingConf(SamplingAlgorithm.MIN, 2, 3), 15, Arrays.asList(AGG.values()));
                extractor.addChunk(getChunk1());
                extractor.addChunk(getChunk2());
                extractor.addChunk(getChunk3());
                extractor.flush();
                Assert.assertEquals(3, extractor.chunkCount());
                Assert.assertEquals(15, extractor.pointCount());
                JsonArray expectedPoints = new JsonArray();
                expectedPoints.add(new JsonArray(Arrays.asList(1.0, 1477895624866L)));
                expectedPoints.add(new JsonArray(Arrays.asList(2.0, 1477917224866L)));
                JsonObject aggregation = new JsonObject();
                aggregation.put(MIN.toString(), 1.0);
                aggregation.put(MAX.toString(), 3.0);
                aggregation.put(SUM.toString(), 24.0);
                aggregation.put(COUNT.toString(), 15.0);
                aggregation.put(AVG.toString(), 1.6);
                Assert.assertEquals(new JsonObject()
                                        .put(TIMESERIE_POINT, expectedPoints)
                                        .put(TIMESERIE_AGGS, aggregation)
                                , extractor.getTimeSeries());
            }



    @Test
    public void testMinSampler2() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterUsingPreAgg(
                 Long.MIN_VALUE, Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.MIN, 2, 3),
                12, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk2());
        extractor.addChunk(getChunk3());
        extractor.addChunk(getChunk4());
        extractor.addChunk(getChunk5());
        extractor.flush();
        Assert.assertEquals(4, extractor.chunkCount());
        Assert.assertEquals(12, extractor.pointCount());
        JsonArray expectedPoints = new JsonArray();
        expectedPoints.add(new JsonArray(Arrays.asList(2.0, START_CHUNK_2)));
        expectedPoints.add(new JsonArray(Arrays.asList(4.0, 1477917224870L)));
        JsonObject aggregation = new JsonObject();
        aggregation.put(MIN.toString(), 2.0);
        aggregation.put(MAX.toString(), 5.0);
        aggregation.put(SUM.toString(), 42.0);
        aggregation.put(COUNT.toString(), 12.0);
        aggregation.put(AVG.toString(), 3.5);
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_POINT, expectedPoints)
                        .put(TIMESERIE_AGGS, aggregation)
                , extractor.getTimeSeries());
    }


    @Test
    public void testAggsWithSeveralFlush() {
        TimeSeriesExtracter extractor = new TimeSeriesExtracterImpl(
                Long.MIN_VALUE , Long.MAX_VALUE,
                new SamplingConf(SamplingAlgorithm.AVERAGE, 2, 3),
                21, Arrays.asList(AGG.values()));
        extractor.addChunk(getChunk1());
        extractor.flush();
        extractor.addChunk(getChunk2());
        extractor.flush();
        extractor.addChunk(getChunk3());
        extractor.flush();
        extractor.addChunk(getChunk4());
        extractor.flush();
        extractor.addChunk(getChunk5());
        extractor.flush();
        Assert.assertEquals(5, extractor.chunkCount());
        Assert.assertEquals(21, extractor.pointCount());
        JsonObject aggregation = new JsonObject();
        aggregation.put(MIN.toString(), 1.0);
        aggregation.put(MAX.toString(), 5.0);
        aggregation.put(SUM.toString(), 51.0);
        aggregation.put(COUNT.toString(), 21.0);
        aggregation.put(AVG.toString(), 2.4);
        JsonObject result = extractor.getTimeSeries();
        Assert.assertEquals(new JsonObject()
                        .put(TIMESERIE_AGGS, aggregation)
                        .getJsonObject(TIMESERIE_AGGS),
                result.getJsonObject(TIMESERIE_AGGS));
    }
}
