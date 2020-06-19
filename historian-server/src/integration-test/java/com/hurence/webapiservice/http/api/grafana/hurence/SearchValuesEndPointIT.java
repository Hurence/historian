package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralVersion0SolrInjector;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.logisland.record.Point;
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

import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class SearchValuesEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchValuesEndPointIT.class);
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
                new Point(0, 1, 1)
        ));
        chunkTempbUsine1Sensor3.addTag("sensor", "sensor_3");
        chunkTempbUsine1Sensor3.addTag("usine", "usine_1");
        injector.addChunk(chunkTempbUsine1Sensor3);

        ChunkModeleVersion0 chunkTempaUsine1Sensor1 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point(0, 2, 2),
                new Point(0, 3, 3),
                new Point(0, 4, 4)
        ));
        chunkTempaUsine1Sensor1.addTag("sensor", "sensor_1");
        chunkTempaUsine1Sensor1.addTag("usine", "usine_1");
        injector.addChunk(chunkTempaUsine1Sensor1);

        ChunkModeleVersion0 chunkTempaUsine1Sensor2 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point(0, 5, 3)
        ));
        chunkTempaUsine1Sensor2.addTag("sensor", "sensor_2");
        chunkTempaUsine1Sensor2.addTag("usine", "usine_1");
        injector.addChunk(chunkTempaUsine1Sensor2);

        ChunkModeleVersion0 chunkTempaUsine2Sensor3 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point(0, 6, 4)
        ));
        chunkTempaUsine2Sensor3.addTag("sensor", "sensor_3");
        chunkTempaUsine2Sensor3.addTag("usine", "usine_2");
        injector.addChunk(chunkTempaUsine2Sensor3);

        ChunkModeleVersion0 chunkTempaUsine1 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point(0, 7, 5)
        ));
        chunkTempaUsine1.addTag("usine", "usine_1");
        injector.addChunk(chunkTempaUsine1);

        ChunkModeleVersion0 chunkTempaUsine3 = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point(0, 7, 5)
        ));
        chunkTempaUsine1.addTag("usine", "usine_3");
        injector.addChunk(chunkTempaUsine3);

        ChunkModeleVersion0 chunkTempaNoUsine = ChunkModeleVersion0.fromPoints("temp_a", Arrays.asList(
                new Point(0, 7, 5)
        ));
        chunkTempaUsine1.addTag("usine", "no_usine");
        injector.addChunk(chunkTempaNoUsine);

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
    public void testSearchSensorsValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchSensorValues/request.json",
                        "/http/grafana/hurence/searchValues/testSearchSensorValues/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchUsinesValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchUsineValues/request.json",
                        "/http/grafana/hurence/searchValues/testSearchUsineValues/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchNamesValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchNameValues/request.json",
                        "/http/grafana/hurence/searchValues/testSearchNameValues/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchUsineValuesWithQuery(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithQuery/request.json",
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchUsineValuesWithLimit(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithLimit/request.json",
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithLimit/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

}