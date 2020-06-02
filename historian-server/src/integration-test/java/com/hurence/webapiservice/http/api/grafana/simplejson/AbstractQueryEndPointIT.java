package com.hurence.webapiservice.http.api.grafana.simplejson;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.util.HttpITHelper;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public abstract class AbstractQueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractQueryEndPointIT.class);
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        assertHelper = new AssertResponseGivenRequestHelper(HttpITHelper.buildWebClient(vertx), HttpServerVerticle.SIMPLE_JSON_GRAFANA_QUERY_API_ENDPOINT);
    }

    @AfterAll
    public static void closeVertx(Vertx vertx, VertxTestContext context) {
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQuery(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/test1/request.json",
                "/http/grafana/simplejson/query/extract-algo/test1/expectedResponse.json");
    }

    //TODO use parametric tests so that we can add new tests by adding files without touching code
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints5(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax5/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax5/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints6(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax6/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax6/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints7(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax7/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax7/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints8(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax8/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax8/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints9(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax9/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax9/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints10(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax10/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax10/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints15(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax15/request.json",
                "/http/grafana/simplejson/query/extract-algo/testMaxDataPoints/testMax15/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageDefaultBucket(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testWithAlgo/average/default-bucket/request.json",
                "/http/grafana/simplejson/query/extract-algo/testWithAlgo/average/default-bucket/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageBucketSize2(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testWithAlgo/average/bucket-2/request.json",
                "/http/grafana/simplejson/query/extract-algo/testWithAlgo/average/bucket-2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageBucketSize3(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/extract-algo/testWithAlgo/average/bucket-3/request.json",
                "/http/grafana/simplejson/query/extract-algo/testWithAlgo/average/bucket-3/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMetricWithSpace(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/metric-name/with-space/request.json",
                "/http/grafana/simplejson/query/metric-name/with-space/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMetricWithSpace2(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/simplejson/query/metric-name/with-space2/request.json",
                "/http/grafana/simplejson/query/metric-name/with-space2/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
