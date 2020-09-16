package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralVersion0SolrInjector;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.model.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.reactivex.Completable;
import io.vertx.core.json.JsonArray;
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ComplexQueryEndPointVersion0IT {

    private static Logger LOGGER = LoggerFactory.getLogger(ComplexQueryEndPointVersion0IT.class);
    private static WebClient webClient;

    @BeforeAll
    public static void initSolrAnderticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        Completable solrAndVerticlesDeployed = initSolrAndVerticles(container, vertx, context);
        Completable injectIntoSolr = Completable.fromCallable(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                injectChunksIntoSolr(client);
                return null;
            }
        });
        solrAndVerticlesDeployed
                .andThen(injectIntoSolr)
                .subscribe(context::completeNow,
                        context::failNow);


    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        GeneralVersion0SolrInjector injector = new GeneralVersion0SolrInjector();
        ChunkModeleVersion0 chunkTempbUsine1Sensor3 = ChunkModeleVersion0.fromPoints("temp_b", Arrays.asList(
                new Point( 1, 1)
        ));
        chunkTempbUsine1Sensor3.addTag("sensor", "sensor_3");
        chunkTempbUsine1Sensor3.addTag("usine", "usine_1");
        injector.addChunk(chunkTempbUsine1Sensor3);

        ChunkModeleVersion0 chunkTempaUsine1Sensor1 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point( 2, 2),
                new Point( 3, 3),
                new Point( 4, 4)
        ));
        chunkTempaUsine1Sensor1.addTag("sensor", "sensor_1");
        chunkTempaUsine1Sensor1.addTag("usine", "usine_1");
        injector.addChunk(chunkTempaUsine1Sensor1);

        ChunkModeleVersion0 chunkTempaUsine1Sensor2 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point( 5, 3)
        ));
        chunkTempaUsine1Sensor2.addTag("sensor", "sensor_2");
        chunkTempaUsine1Sensor2.addTag("usine", "usine_1");
        injector.addChunk(chunkTempaUsine1Sensor2);

        ChunkModeleVersion0 chunkTempaUsine2Sensor3 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point( 6, 4)
        ));
        chunkTempaUsine2Sensor3.addTag("sensor", "sensor_3");
        chunkTempaUsine2Sensor3.addTag("usine", "usine_2");
        injector.addChunk(chunkTempaUsine2Sensor3);

        ChunkModeleVersion0 chunkTempaUsine1 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point( 7, 5)
        ));
        chunkTempaUsine1.addTag("usine", "usine_1");
        injector.addChunk(chunkTempaUsine1);

        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static Completable initSolrAndVerticles(DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0);
        SolrITHelper.addFieldToChunkSchema(container, "usine");
        SolrITHelper.addFieldToChunkSchema(container, "sensor");
        return HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx).ignoreElement();
    }

    @BeforeAll
    public static void initWebclient(Vertx vertx) {
        webClient = HttpITHelper.buildWebClient(vertx);
    }

    @AfterAll
    public static void closeWebClientAndVerticles(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQuery(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test1/request.json",
                        "/http/grafana/hurence/query/complex/test1/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithNoTags(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test2/request.json",
                        "/http/grafana/hurence/query/complex/test2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithAggregations(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test3/request.json",
                        "/http/grafana/hurence/query/complex/test3/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithDifferentTags(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test4/request.json",
                        "/http/grafana/hurence/query/complex/test4/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithRefIds(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test5/request.json",
                        "/http/grafana/hurence/query/complex/test5/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}
