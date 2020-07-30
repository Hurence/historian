package com.hurence.webapiservice.historian.compatibility;

import com.hurence.historian.compatibility.ChunkSchemaCompatibilityUtil;
import com.hurence.historian.modele.FieldNamesInsideHistorianService;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.webapiservice.http.api.grafana.QueryRequestParserTest;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChunkSchemaCompatibilityUtilTest {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryRequestParserTest.class);
    @Test
    public void testTransformingChunkFirst() {
        final JsonObject inputJson = new JsonObject(
                "{" +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.NAME + "\":\"metric_10_chunk\"," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START + "\":1," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE + "\":2," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST + "\":[1.0]," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM + "\":[1.0]," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END + "\":2" +
                        "}");
        final JsonObject expectedJson = new JsonObject(
                "{" +
                        "\"" + FieldNamesInsideHistorianService.NAME + "\":\"metric_10_chunk\"," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_START + "\":1," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_COUNT + "\":2," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_FIRST + "\":1.0," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_SUM + "\":1.0," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_END + "\":2" +
                        "}");
        ChunkSchemaCompatibilityUtil.convertEVOA0ToInternalChunk(inputJson);
        assertEquals(expectedJson, inputJson);
    }

    @Test
    public void testTransformingChunkFirstEmptyArray() {
        final JsonObject inputJson = new JsonObject(
                "{" +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.NAME + "\":\"metric_10_chunk\"," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START + "\":1," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE + "\":2," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST + "\":[]," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM + "\":[]," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END + "\":2" +
                        "}");
        final JsonObject expectedJson = new JsonObject(
                "{" +
                        "\"" + FieldNamesInsideHistorianService.NAME + "\":\"metric_10_chunk\"," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_START + "\":1," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_COUNT + "\":2," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_FIRST + "\":[]," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_SUM + "\":[]," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_END + "\":2" +
                        "}");
        ChunkSchemaCompatibilityUtil.convertEVOA0ToInternalChunk(inputJson);
        assertEquals(expectedJson, inputJson);
    }

    @Test
    public void testNotThrowingErrorIfAlreadyOfCorrectType() {
        final JsonObject inputJson = new JsonObject(
                "{" +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.NAME + "\":\"metric_10_chunk\"," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START + "\":1," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE + "\":2," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST + "\":1.0," +
                        "\"" + HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END + "\":2" +
                        "}");
        final JsonObject expectedJson = new JsonObject(
                "{" +
                        "\"" + FieldNamesInsideHistorianService.NAME + "\":\"metric_10_chunk\"," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_START + "\":1," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_COUNT + "\":2," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_FIRST + "\":1.0," +
                        "\"" + FieldNamesInsideHistorianService.CHUNK_END + "\":2" +
                        "}");
        ChunkSchemaCompatibilityUtil.convertEVOA0ToInternalChunk(inputJson);
        assertEquals(expectedJson, inputJson);
    }
}
