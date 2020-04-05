package com.hurence.webapiservice.historian.compatibility;

import com.hurence.webapiservice.http.grafana.QueryRequestParserTest;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonSchemaCompatibilityUtilTest {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryRequestParserTest.class);
    @Test
    public void testTransformingChunkFirst() {
        JsonObject requestBody = new JsonObject(
                "{" +
                        "\"name\":\"metric_10_chunk\"," +
                        "\"chunk_start\":1," +
                        "\"chunk_size\":2," +
                        "\"chunk_first\":[1.0]," +
                        "\"chunk_end\":2" +
                "}");
        final JsonObject expectedJson = new JsonObject(
                "{" +
                        "\"name\":\"metric_10_chunk\"," +
                        "\"chunk_start\":1," +
                        "\"chunk_size\":2," +
                        "\"chunk_first\": 1.0," +
                        "\"chunk_end\":2" +
                        "}");
        final JsonObject jsonConvertedToCurrentSchema = JsonSchemaCompatibilityUtil.convertSchema0ToCurrent(requestBody);
        assertEquals(expectedJson, jsonConvertedToCurrentSchema);
    }

    @Test
    public void testNotThrowingErrorIfAlreadyOfCorrectType() {
        JsonObject requestBody = new JsonObject(
                "{" +
                        "\"name\":\"metric_10_chunk\"," +
                        "\"chunk_start\":1," +
                        "\"chunk_size\":2," +
                        "\"chunk_first\": 1.0," +
                        "\"chunk_end\":2" +
                        "}");
        final JsonObject expectedJson = new JsonObject(
                "{" +
                        "\"name\":\"metric_10_chunk\"," +
                        "\"chunk_start\":1," +
                        "\"chunk_size\":2," +
                        "\"chunk_first\": 1.0," +
                        "\"chunk_end\":2" +
                        "}");
        final JsonObject jsonConvertedToCurrentSchema = JsonSchemaCompatibilityUtil.convertSchema0ToCurrent(requestBody);
        assertEquals(expectedJson, jsonConvertedToCurrentSchema);
    }
}
