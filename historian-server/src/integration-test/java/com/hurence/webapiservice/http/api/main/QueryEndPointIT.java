package com.hurence.webapiservice.http.api.main;

import com.hurence.historian.modele.ChunkModele;
import com.hurence.historian.solr.injector.GeneralSolrInjector;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.http.HttpServerVerticle.*;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolrAndVerticles(client, container, vertx, context);
        injectChunksIntoSolr(client, vertx);
    }

    public static void injectChunksIntoSolr(SolrClient client, Vertx vertx) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        GeneralSolrInjector injector = new GeneralSolrInjector();
        ChunkModele chunkMetric1TagSensor1 = ChunkModele.fromPoints("metric_1", Arrays.asList(
                new Point(0 , 1, 1.0),
                new Point(0 , 2, 2.0)
        ));
        chunkMetric1TagSensor1.addTag("sensor", "sensor_1");
        injector.addChunk(chunkMetric1TagSensor1);
        ChunkModele chunkMetric1TagSensor2 = ChunkModele.fromPoints("metric_1", Arrays.asList(
                new Point(0 , 3, 1.0),
                new Point(0 , 4, 2.0)
        ));
        chunkMetric1TagSensor2.addTag("sensor", "sensor_2");
        injector.addChunk(chunkMetric1TagSensor2);
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/query");
    }

    public static void initSolrAndVerticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    public void testMinimalQuery(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<>(MAIN_QUERY_ENDPOINT,
                        "/http/mainapi/query/minimal/request.json",
                        "/http/mainapi/query/minimal/response.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    //TODO use parametric tests so that we can add new tests by adding files without touching code
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testTagsQuery(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<>(MAIN_QUERY_ENDPOINT,
                        "/http/mainapi/query/tags/request.json",
                        "/http/mainapi/query/tags/response.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
