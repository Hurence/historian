package com.hurence.webapiservice.http.api.grafana.hurence;

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
        assertHelper = new AssertResponseGivenRequestHelper(HttpITHelper.buildWebClient(vertx), HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQuery(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/test1/request.json",
                "/http/grafana/hurence/query/test1/expectedResponse.json");
    }

    //TODO use parametric tests so that we can add new tests by adding files without touching code
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints5(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax5/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax5/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints6(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax6/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax6/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints7(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax7/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax7/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints8(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax8/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax8/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints9(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax9/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax9/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints10(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax10/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax10/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints15(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testMaxDataPoints/testMax15/request.json",
                "/http/grafana/hurence/query/testMaxDataPoints/testMax15/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageDefaultBucket(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithAlgo/average/default-bucket/request.json",
                "/http/grafana/hurence/query/testWithAlgo/average/default-bucket/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageBucketSize2(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithAlgo/average/bucket-2/request.json",
                "/http/grafana/hurence/query/testWithAlgo/average/bucket-2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageBucketSize3(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithAlgo/average/bucket-3/request.json",
                "/http/grafana/hurence/query/testWithAlgo/average/bucket-3/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMetricWithSpace(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/metric-name/with-space/request.json",
                "/http/grafana/hurence/query/metric-name/with-space/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMetricWithSpace2(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/metric-name/with-space2/request.json",
                "/http/grafana/hurence/query/metric-name/with-space2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithAggregation(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/aggregations/testQueryWithAggregation/request.json",
                "/http/grafana/hurence/query/aggregations/testQueryWithAggregation/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithALLAggregation(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/aggregations/testQueryWithALLAggregation/request.json",
                "/http/grafana/hurence/query/aggregations/testQueryWithALLAggregation/expectedResponse.json");
    }

    /**
     * bug found the 23/06/2020. Not working when request is like
     * <pre>"names" : [{ "name" : "metric" }]</pre>
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithObjectName(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testNames/objectNameWithoutTags/request.json",
                "/http/grafana/hurence/query/testNames/objectNameWithoutTags/expectedResponse.json");
    }

    /**
     * bug found the 03/09/2020. Not working with special characters !
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithSpecialCharacters(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/metric-name/special-characters/request.json",
                "/http/grafana/hurence/query/metric-name/special-characters/expectedResponse.json");
    }


    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
