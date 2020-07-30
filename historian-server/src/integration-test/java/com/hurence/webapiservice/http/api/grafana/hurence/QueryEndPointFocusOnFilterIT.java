package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.Version0SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.http.api.grafana.GrafanaApiVersion;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.WebClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.HistorianVerticle.CONFIG_SCHEMA_VERSION;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnFilterIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusOnFilterIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0);
        SolrITHelper.addFieldToChunkSchema(container, "usine");
        SolrITHelper.addFieldToChunkSchema(container, "pays");
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new Version0SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(
                "metric_to_filter",
                Arrays.asList(
                        new HashMap<String, String>(){{
                            put("pays", "Berlin");

                        }},
                        new HashMap<String, String>(){{
                            put("pays", "France");

                        }},
                        new HashMap<String, String>(){{
                            put("usine", "usine_1");
                            put("pays", "Berlin");
                        }},
                        new HashMap<String, String>(){{
                            put("pays", "France");
                        }}
                ),
                Arrays.asList(
                        Arrays.asList(
                                new PointImpl( 1477895624866L, 1.0),
                                new PointImpl( 1477916224866L, 1.0),
                                new PointImpl( 1477917224866L, 1.0)
                        ),
                        Arrays.asList(
                                new PointImpl( 1477917224868L, 2.0),
                                new PointImpl( 1477917224886L, 2.0)
                        ),
                        Arrays.asList(
                                new PointImpl( 1477917224980L, 3.0),
                                new PointImpl( 1477917224981L, 3.0)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                new PointImpl( 1477917224988L, 4.0),
                                new PointImpl( 1477917324988L, 4.0)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT);
        JsonObject historianConf = new JsonObject()
                .put(CONFIG_SCHEMA_VERSION,
                        SchemaVersion.VERSION_0.toString());
        HttpWithHistorianSolrITHelper.deployHttpAndCustomHistorianVerticle(container, vertx, historianConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testFilterOnTagsBerlin(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/tags/berlin/request.json",
                "/http/grafana/hurence/query/tags/berlin/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testFilterOnTagsFrance(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/tags/france/request.json",
                "/http/grafana/hurence/query/tags/france/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testFilterOnTagsBerlinAndFrance(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/tags/franceAndBerlin/request.json",
                "/http/grafana/hurence/query/tags/franceAndBerlin/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
