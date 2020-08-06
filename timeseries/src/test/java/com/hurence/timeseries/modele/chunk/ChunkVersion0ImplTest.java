package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.converter.PointsToChunkVersion0;
import com.hurence.timeseries.modele.points.Point;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ChunkVersion0ImplTest {

    private static final Logger logger = LoggerFactory.getLogger(ChunkVersion0ImplTest.class);

    @Test
    public void testTruncate() {
        Map<String, String> tags = new HashMap<String, String>(){{
            put("couNtry", "France");
            put("usine", "usine 2 ;;Alpha go");
        }};
        PointsToChunkVersion0 converter = new PointsToChunkVersion0("test");
        ChunkVersion0 chunk = converter.buildChunk("metric 1",
                Arrays.asList(
                    Point.fromValue(1, 1),
                        Point.fromValue(10, 2),
                        Point.fromValue(100, 3),
                        Point.fromValue(200, 4),
                        Point.fromValue(300, 5),
                        Point.fromValue(500, 6),
                        Point.fromValue(600, 7),
                        Point.fromValue(800, 8),
                        Point.fromValue(1000, 9),
                        Point.fromValue(1111, 10)
                ),
                    tags
                );
        //        origin chunk
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10,chunk.getLast());
        Assertions.assertEquals(Collections.emptyList(),chunk.getCompactionsRunning());
        Assertions.assertEquals("test",chunk.getOrigin());
        Assertions.assertEquals(false,chunk.getOutlier());
        Assertions.assertEquals(true,chunk.getTrend());
        Assertions.assertEquals(3.0276503540974917,chunk.getStddev());
        Assertions.assertEquals("aabcddefgg",chunk.getSax());
        Assertions.assertEquals(5.5,chunk.getAvg());
        Assertions.assertEquals(SchemaVersion.VERSION_0,chunk.getVersion());
        Assertions.assertEquals("1970-01-01",chunk.getDay());
        Assertions.assertEquals("2c6191ea9a9abe5443b73da0c3c072819d5b6e6f2d2c379513067356e2dae019",chunk.getId());
        Assertions.assertEquals(1,chunk.getFirst());
        Assertions.assertEquals(10,chunk.getMax());
        Assertions.assertEquals(1,chunk.getMin());
        Assertions.assertEquals(1,chunk.getMonth());
        Assertions.assertEquals("metric 1",chunk.getName());
        Assertions.assertEquals(1,chunk.getStart());
        Assertions.assertEquals(55,chunk.getSum());
        Assertions.assertEquals(1111,chunk.getEnd());
        Assertions.assertEquals(tags,chunk.getTags());
        Assertions.assertEquals(1970,chunk.getYear());
        //        truncated chunk
        ChunkVersion0 truncatedChunk = chunk.truncate(301, 799);
        Assertions.assertEquals(2,truncatedChunk.getCount());
        Assertions.assertEquals(7,truncatedChunk.getLast());
        Assertions.assertEquals(Collections.emptyList(),truncatedChunk.getCompactionsRunning());
        Assertions.assertEquals("ChunkTruncater",truncatedChunk.getOrigin());
        Assertions.assertEquals(false,truncatedChunk.getOutlier());
        Assertions.assertEquals(true,truncatedChunk.getTrend());
        Assertions.assertEquals(0.7071067811865476,truncatedChunk.getStddev());
        Assertions.assertEquals("ab",truncatedChunk.getSax());
        Assertions.assertEquals(6.5,truncatedChunk.getAvg());
        Assertions.assertEquals(SchemaVersion.VERSION_0,truncatedChunk.getVersion());
        Assertions.assertEquals("1970-01-01",truncatedChunk.getDay());
        Assertions.assertEquals("e5fc729560a76e72c439d9c614cf77ce876dc2da0175f563a542d8d5c90383a3",truncatedChunk.getId());
        Assertions.assertEquals(6,truncatedChunk.getFirst());
        Assertions.assertEquals(7,truncatedChunk.getMax());
        Assertions.assertEquals(6,truncatedChunk.getMin());
        Assertions.assertEquals(1,truncatedChunk.getMonth());
        Assertions.assertEquals("metric 1",truncatedChunk.getName());
        Assertions.assertEquals(500,truncatedChunk.getStart());
        Assertions.assertEquals(13,truncatedChunk.getSum());
        Assertions.assertEquals(600,truncatedChunk.getEnd());
        Assertions.assertEquals(tags,truncatedChunk.getTags());
        Assertions.assertEquals(1970,truncatedChunk.getYear());
    }
}
