package com.hurence.webapiservice.http.api.grafana;

import com.hurence.webapiservice.http.api.grafana.model.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.QueryRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import static com.hurence.webapiservice.http.api.grafana.model.QueryRequestParam.DEFAULT_BUCKET_SIZE;
import static com.hurence.webapiservice.http.api.grafana.model.QueryRequestParam.DEFAULT_SAMPLING_ALGORITHM;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryRequestParserTest {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryRequestParserTest.class);

    public static String REQUEST_ID = "requestId";
    public static String TIMEZONE = "timezone";
    public static String PANELID = "panelId";
    public static String DASHBOARD_ID = "dashboardId";
    public static String RANGE = "range";
    public static String FROM = "from";
    public static String TO = "to";
    public static String RAW = "raw";
    public static String TARGETS = "targets";
    public static String TARGET = "target";
    public static String REFID = "refId";
    public static String TYPE = "type";
    public static String MAXDATAPOINTS = "maxDataPoints";
    public static String ADHOCFILTERS = "adhocFilters";
    public static String SCOPEDVARS = "scopedVars";
    public static String INTERVAL = "__interval";
    public static String TEXT = "text";
    public static String VALUE = "value";
    public static String INTERVAL_MS = "__interval_ms";
    public static String STARTTIME = "startTime";
    public static String RANGERAW = "rangeRaw";

    @Test
    public void testParsingRequestFromGrafana1() {
        JsonObject requestBody = new JsonObject()
                .put(REQUEST_ID, "Q108")
                .put(TIMEZONE, "")
                .put(PANELID, 2)
                .put(DASHBOARD_ID, 2)
                .put(RANGE, new JsonObject()
                        .put(FROM, "2019-11-14T02:56:53.285Z")
                        .put(TO, "2019-11-14T08:56:53.285Z")
                        .put(RAW, new JsonObject()
                                .put(FROM, "now-6h")
                                .put(TO, "now")
                        )
                )
                .put(TARGETS, new JsonArray()
                        .add(new JsonObject().put(TARGET, "speed")
                                .put(REFID, "A")
                                .put(TYPE, "timeserie"))
                        .add(new JsonObject().put(TARGET, "pressure")
                                .put(REFID, "B")
                                .put(TYPE, "timeserie"))
                        .add(new JsonObject().put(TARGET, "rotation")
                                .put(REFID, "C")
                                .put(TYPE, "timeserie"))
                )
                .put(MAXDATAPOINTS, 844)
                .put(SCOPEDVARS, new JsonObject()
                        .put(INTERVAL, new JsonObject()
                                .put(TEXT, "2020-2-14T04:43:14.070Z")
                                .put(VALUE, "2020-2-14T07:43:14.070Z"))
                        .put(INTERVAL_MS, new JsonObject()
                                .put(TEXT, "2020-2-14T04:43:14.070Z")
                                .put(VALUE, "2020-2-14T07:43:14.070Z")))
                .put(STARTTIME, 1573721813291L)
                .put(RANGERAW, new JsonObject()
                        .put(FROM, "now-6h")
                        .put(TO, "now")
                )
                .put(ADHOCFILTERS, new JsonArray()
                );
        final QueryRequestParser queryRequestParser = new QueryRequestParser();
        final QueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1573700213285L, request.getFrom());
        assertEquals(1573721813285L, request.getTo());
        assertEquals(844, request.getSamplingConf().getMaxPoint());
        assertEquals(DEFAULT_BUCKET_SIZE, request.getSamplingConf().getBucketSize());
        assertEquals(DEFAULT_SAMPLING_ALGORITHM, request.getSamplingConf().getAlgo());
        assertEquals(Collections.emptyList(), request.getAggs());
        assertEquals(Arrays.asList("speed", "pressure", "rotation"), request.getMetricNames());
        assertEquals(Collections.emptyMap(), request.getTags());
    }

    /**
     * a static SimpleDateFormat was causing trouble, so this test check this problem.
     */
    @Test
    public void testParsingRequestSupportMultiThreaded() {
        JsonObject requestBody = new JsonObject()
                .put(REQUEST_ID, "Q108")
                .put(TIMEZONE, "")
                .put(PANELID, 2)
                .put(DASHBOARD_ID, 2)
                .put(RANGE, new JsonObject()
                        .put(FROM, "2019-11-14T02:56:53.285Z")
                        .put(TO, "2019-11-14T08:56:53.285Z")
                        .put(RAW, new JsonObject()
                                .put(FROM, "now-6h")
                                .put(TO, "now")
                        )
                )
                .put(TARGETS, new JsonArray()
                        .add(new JsonObject().put(TARGET, "speed")
                                .put(REFID, "A")
                                .put(TYPE, "timeserie"))
                        .add(new JsonObject().put(TARGET, "pressure")
                                .put(REFID, "B")
                                .put(TYPE, "timeserie"))
                        .add(new JsonObject().put(TARGET, "rotation")
                                .put(REFID, "C")
                                .put(TYPE, "timeserie"))
                )
                .put(MAXDATAPOINTS, 844)
                .put(SCOPEDVARS, new JsonObject()
                        .put(INTERVAL, new JsonObject()
                                .put(TEXT, "2020-2-14T04:43:14.070Z")
                                .put(VALUE, "2020-2-14T07:43:14.070Z"))
                        .put(INTERVAL_MS, new JsonObject()
                                .put(TEXT, "2020-2-14T04:43:14.070Z")
                                .put(VALUE, "2020-2-14T07:43:14.070Z")))
                .put(STARTTIME, 1573721813291L)
                .put(RANGERAW, new JsonObject()
                        .put(FROM, "now-6h")
                        .put(TO, "now")
                )
                .put(ADHOCFILTERS, new JsonArray()
                );
        IntStream
                .range(0, 10)
                .parallel()
                .forEach(i -> {
                    final QueryRequestParser queryRequestParser = new QueryRequestParser();
                    queryRequestParser.parseRequest(requestBody);
        });
    }


    @Test
    public void testParsingMinimalRequest() {
        JsonObject requestBody = new JsonObject()
                .put(TARGETS, new JsonArray()
                        .add(new JsonObject().put(TARGET, "speed"))
                );
        final QueryRequestParser queryRequestParser = new QueryRequestParser();
        final QueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(QueryRequestParam.DEFAULT_FROM, request.getFrom());
        assertEquals(QueryRequestParam.DEFAULT_TO, request.getTo());
        assertEquals(QueryRequestParam.DEFAULT_MAX_DATAPOINTS, request.getSamplingConf().getMaxPoint());
        assertEquals(DEFAULT_BUCKET_SIZE, request.getSamplingConf().getBucketSize());
        assertEquals(DEFAULT_SAMPLING_ALGORITHM, request.getSamplingConf().getAlgo());
        assertEquals(Collections.emptyList(), request.getAggs());
        assertEquals(Arrays.asList("speed"), request.getMetricNames());
        assertEquals(Collections.emptyMap(), request.getTags());
    }

    @Test
    public void testparsingErrorEmptyRequest() {
        JsonObject requestBody = new JsonObject("{}");
        final QueryRequestParser queryRequestParser = new QueryRequestParser();
        Assertions.assertThrows(NullPointerException.class, () -> {
            final QueryRequestParam request = queryRequestParser.parseRequest(requestBody);
        });
    }
}
