package com.hurence.timeseries.modele.chunk;

import com.hurence.timeseries.converter.PointsToChunkVersion0;
import com.hurence.timeseries.modele.points.Point;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;

public class ChunkVersion0ImplTest {

    private static final Logger logger = LoggerFactory.getLogger(ChunkVersion0ImplTest.class);

    @Test
    public void testTruncate() {
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
                new HashMap<String, String>(){{
                    put("couNtry", "France");
                    put("usine", "usine 2 ;;Alpha go");
                }});
        //        origin chunk
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        Assertions.assertEquals(10L,chunk.getCount());
        //        truncated chunk
        Chunk truncatedChunk = chunk.truncate(301, 799);
        Assertions.assertEquals(2L, truncatedChunk.getCount());
    }
}
