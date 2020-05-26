package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.modele.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginQueryRequestParser;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hurence.webapiservice.http.api.grafana.GrafanaHurenceDatasourcePliginApiImpl.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryRequestParserTest {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryRequestParserTest.class);

    @Test
    public void testParsingRequest() {
        JsonObject requestBody = new JsonObject();
        JsonPointer.from(BUCKET_SIZE_JSON_PATH)
                .writeJson(requestBody, 100, true);
        JsonPointer.from(FROM_JSON_PATH)
                .writeJson(requestBody, "2016-10-31T06:33:44.866Z", true);
        JsonPointer.from(TO_JSON_PATH)
                .writeJson(requestBody, "2020-10-31T12:33:44.866Z", true);
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        JsonPointer.from(FORMAT_JSON_PATH)
                .writeJson(requestBody, "json2", true);
        JsonPointer.from(TAGS_JSON_PATH)
                .writeJson(requestBody, new HashMap<String, String>() {{
                    put("sensor", "sensor_1");
                }}, true);
        JsonPointer.from(SAMPLING_ALGO_JSON_PATH)
                .writeJson(requestBody, SamplingAlgorithm.MIN.toString(), true);
        JsonPointer.from(REQUEST_ID_JSON_PATH)
                .writeJson(requestBody, "REQUEST_0", true);
        JsonPointer.from(MAX_DATAPOINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);

        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH,MAX_DATAPOINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH);
        final TimeSeriesRequest request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1604147624866L, request.getFrom());
        assertEquals(9223372036854775807L, request.getTo());
        assertEquals(Collections.emptyList(), request.getAggs());
        assertEquals(Arrays.asList("metric_1"), request.getMetricNames());
        assertEquals(new HashMap<String, String>() {{
            put("sensor", "sensor_1");
        }}, request.getTags());
        assertEquals("REQUEST_0", request.getRequestId());
        assertEquals(500, request.getSamplingConf().getMaxPoint());
        assertEquals(100, request.getSamplingConf().getBucketSize());
        assertEquals(SamplingAlgorithm.MIN, request.getSamplingConf().getAlgo());
    }

    /**
     * a static SimpleDateFormat was causing trouble, so this test check this problem.
     */
    @Test
    public void testParsingRequestSupportMultiThreaded() {
        JsonObject requestBody = new JsonObject();
        JsonPointer.from(BUCKET_SIZE_JSON_PATH)
                .writeJson(requestBody, 100, true);
        JsonPointer.from(FROM_JSON_PATH)
                .writeJson(requestBody, "2016-10-31T06:33:44.866Z", true);
        JsonPointer.from(TO_JSON_PATH)
                .writeJson(requestBody, "2020-10-31T12:33:44.866Z", true);
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        JsonPointer.from(FORMAT_JSON_PATH)
                .writeJson(requestBody, "json2", true);
        JsonPointer.from(TAGS_JSON_PATH)
                .writeJson(requestBody, new HashMap<String, String>() {{
                    put("sensor", "sensor_1");
                }}, true);
        JsonPointer.from(SAMPLING_ALGO_JSON_PATH)
                .writeJson(requestBody, SamplingAlgorithm.MIN.toString(), true);
        JsonPointer.from(REQUEST_ID_JSON_PATH)
                .writeJson(requestBody, "REQUEST_0", true);
        JsonPointer.from(MAX_DATAPOINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);
        IntStream
                .range(0, 10)
                .parallel()
                .forEach(i -> {
                    final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                            TO_JSON_PATH,NAMES_JSON_PATH,MAX_DATAPOINTS_JSON_PATH,FORMAT_JSON_PATH,
                            TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH);
                    final TimeSeriesRequest request = queryRequestParser.parseRequest(requestBody);
        });
    }

    @Test
    public void testParsingMinimalRequest() {
        JsonObject requestBody = new JsonObject();
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH,MAX_DATAPOINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH);
        final TimeSeriesRequest request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_FROM, request.getFrom());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_TO, request.getTo());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_MAX_DATAPOINTS, request.getSamplingConf().getMaxPoint());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_BUCKET_SIZE, request.getSamplingConf().getBucketSize());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_SAMPLING_ALGORITHM, request.getSamplingConf().getAlgo());
        assertEquals(Arrays.asList("metric_1"), request.getMetricNames());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_REQUEST_ID, request.getRequestId());
    }

    @Test
    public void testparsingErrorEmptyRequest() {
        JsonObject requestBody = new JsonObject("{}");
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH,MAX_DATAPOINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH);
        Assertions.assertThrows(NullPointerException.class, () -> {
            final TimeSeriesRequest request = queryRequestParser.parseRequest(requestBody);
        });
    }
}
