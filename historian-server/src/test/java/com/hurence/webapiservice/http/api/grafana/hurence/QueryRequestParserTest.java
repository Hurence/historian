package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.modele.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginQueryRequestParser;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

import static com.hurence.webapiservice.http.api.grafana.GrafanaHurenceDatasourcePluginApiImpl.*;
import static com.hurence.webapiservice.modele.AGG.*;
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
        JsonPointer.from(MAX_DATA_POINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);

        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1477895624866L, request.getFrom());
        assertEquals(1604147624866L, request.getTo());
        assertEquals(Collections.emptyList(), request.getAggs());
        assertEquals(new JsonArray(Arrays.asList("metric_1")), request.getMetricNames());
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
        JsonPointer.from(MAX_DATA_POINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);
        IntStream
                .range(0, 10)
                .parallel()
                .forEach(i -> {
                    final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                            TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                            TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
                    final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        });
    }

    @Test
    public void testParsingMinimalRequest() {
        JsonObject requestBody = new JsonObject();
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_FROM, request.getFrom());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_TO, request.getTo());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_MAX_DATAPOINTS, request.getSamplingConf().getMaxPoint());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_BUCKET_SIZE, request.getSamplingConf().getBucketSize());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_SAMPLING_ALGORITHM, request.getSamplingConf().getAlgo());
        assertEquals(new JsonArray(Arrays.asList("metric_1")), request.getMetricNames());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(HurenceDatasourcePluginQueryRequestParam.DEFAULT_REQUEST_ID, request.getRequestId());
    }

    @Test
    public void testparsingErrorEmptyRequest() {
        JsonObject requestBody = new JsonObject("{}");
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        });
    }


    @Test
    public void testParsingRequestWithAggregation() {
        List<AGG> aggrs = new ArrayList<>();
        aggrs.add(MAX);
        aggrs.add(MIN);
        aggrs.add(AVG);
        aggrs.add(COUNT);
        aggrs.add(SUM);

        JsonObject requestBody = new JsonObject();
        JsonPointer.from(FROM_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T02:56:53.285Z", true);
        JsonPointer.from(TO_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T08:56:53.285Z", true);
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        JsonPointer.from(MAX_DATA_POINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);
        JsonPointer.from(AGGREGATION_JSON_PATH)
                .writeJson(requestBody,Arrays.asList(MAX, MIN, AVG, COUNT, SUM), true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1573700213285L, request.getFrom());
        assertEquals(1573721813285L, request.getTo());
        assertEquals(aggrs, request.getAggs());
        assertEquals(new JsonArray(Arrays.asList("metric_1")), request.getMetricNames());
        assertEquals(500, request.getSamplingConf().getMaxPoint());
    }

    @Test
    public void testParsingRequestWithWrongAggregation() {

        JsonObject requestBody = new JsonObject();
        JsonPointer.from(FROM_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T02:56:53.285Z", true);
        JsonPointer.from(TO_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T08:56:53.285Z", true);
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        JsonPointer.from(MAX_DATA_POINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);
        JsonPointer.from(AGGREGATION_JSON_PATH)
                .writeJson(requestBody,Arrays.asList("MA", MIN), true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            queryRequestParser.parseRequest(requestBody);
        });
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            queryRequestParser.parseRequest(requestBody);
        }).getMessage();
        assertEquals(message, "MA is not a recognized aggregation, the accepted aggregations are : " + Arrays.asList(AGG.values()));
    }
    @Test
    public void testParsingRequestWithALLAggregation() {
        JsonObject requestBody = new JsonObject();
        JsonPointer.from(FROM_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T02:56:53.285Z", true);
        JsonPointer.from(TO_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T08:56:53.285Z", true);
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        JsonPointer.from(MAX_DATA_POINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);
        JsonPointer.from(AGGREGATION_JSON_PATH)
                .writeJson(requestBody, true, true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1573700213285L, request.getFrom());
        assertEquals(1573721813285L, request.getTo());
        assertEquals(Arrays.asList(AGG.values()), request.getAggs());
        assertEquals(new JsonArray(Arrays.asList("metric_1")), request.getMetricNames());
        assertEquals(500, request.getSamplingConf().getMaxPoint());
    }

    @Test
    public void testParsingRequestWithWrongBooleanAggregation() {

        JsonObject requestBody = new JsonObject();
        JsonPointer.from(FROM_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T02:56:53.285Z", true);
        JsonPointer.from(TO_JSON_PATH)
                .writeJson(requestBody, "2019-11-14T08:56:53.285Z", true);
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, Arrays.asList("metric_1"), true);
        JsonPointer.from(MAX_DATA_POINTS_JSON_PATH)
                .writeJson(requestBody, 500, true);
        JsonPointer.from(AGGREGATION_JSON_PATH)
                .writeJson(requestBody,false, true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            queryRequestParser.parseRequest(requestBody);
        });
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            queryRequestParser.parseRequest(requestBody);
        }).getMessage();
        assertEquals(message, "'" +AGGREGATION_JSON_PATH+"' json pointer value 'false' is not a valid value here !");
    }

    @Test
    public void testParsingNamesWithComplexTypes() {
        JsonArray names = new JsonArray(Arrays.asList("temp_b",
                new JsonObject().put(HistorianFields.NAME, "temp_a")
                        .put(HistorianFields.TAGS, new JsonObject()
                                .put("sensor", "sensor_1")
                        ),
                new JsonObject().put(HistorianFields.NAME, "temp_a")
                        .put(HistorianFields.TAGS, new JsonObject()
                                .put("sensor", "sensor_2")
                        )
        ));
        JsonObject requestBody = new JsonObject();
        JsonPointer.from(NAMES_JSON_PATH)
                .writeJson(requestBody, names, true);
        JsonPointer.from(TAGS_JSON_PATH)
                .writeJson(requestBody, new HashMap<String, String>() {{
                    put("usine", "usine_1");
                    put("sensor", "sensor_3");
                }}, true);
        final HurenceDatasourcePluginQueryRequestParser queryRequestParser = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH);
        final HurenceDatasourcePluginQueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(names, request.getMetricNames());
        assertEquals(new HashMap<String, String>() {{
            put("usine", "usine_1");
            put("sensor", "sensor_3");
        }}, request.getTags());
    }
}
