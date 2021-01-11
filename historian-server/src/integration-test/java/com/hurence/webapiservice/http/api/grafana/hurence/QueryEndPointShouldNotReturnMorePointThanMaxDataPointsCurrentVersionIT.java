package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesCurrentSerializer;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointShouldNotReturnMorePointThanMaxDataPointsCurrentVersionIT {

    private static WebClient webClient;
    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointShouldNotReturnMorePointThanMaxDataPointsCurrentVersionIT.class);

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        webClient = HttpITHelper.buildWebClient(vertx);
    }

    @AfterAll
    public static void closeVertx(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
        initVerticlesThenFinish(container, vertx, context);
    }

    private static void initSolr(DockerComposeContainer container) throws InterruptedException, SolrServerException, IOException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
        SolrITHelper.addFieldToChunkSchema(container, "sensor");
        SolrITHelper.addFieldToChunkSchema(container, "code_install");
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }


    public static void initVerticlesThenFinish(DockerComposeContainer container, Vertx vertx, VertxTestContext context)  {
        JsonObject historianConf = buildHistorianConf();
        HttpWithHistorianSolrITHelper
                .deployHttpAndCustomHistorianVerticle(container, vertx, historianConf)
                .subscribe(id -> {
                            context.completeNow();
                        },
                        t -> context.failNow(t));
    }

    public static JsonObject buildHistorianConf() {
        JsonObject historianConf = new JsonObject();
        return historianConf;
    }

    public static SolrInjector buildInjector() throws IOException {
        List<Measure> expectedMeasures1 = Arrays.asList(
                Measure.fromValue( 1477895624866L, 1),
                Measure.fromValue( 1477895624867L, 2));
        List<Measure> expectedMeasures2 = Arrays.asList(
                Measure.fromValue( 1477895624868L, 3),
                Measure.fromValue( 1477895624869L, 4));
        List<Measure> expectedMeasures3 = Arrays.asList(
                Measure.fromValue( 1477895624870L, 5),
                Measure.fromValue( 1477895624871L, 6));
        List<Measure> expectedMeasures4 = Arrays.asList(
                Measure.fromValue( 1477895624872L, 7),
                Measure.fromValue( 1477895624873L, 8));
        List<Measure> expectedMeasures5 = Arrays.asList(
                Measure.fromValue( 1477895624874L, 9),
                Measure.fromValue( 1477895624875L, 10));
        List<Measure> expectedMeasures6 = Arrays.asList(
                Measure.fromValue( 1477895624876L, 11),
                Measure.fromValue( 1477895624877L, 12));
        List<Measure> expectedMeasures7 = Arrays.asList(
                Measure.fromValue( 1477895624878L, 13),
                Measure.fromValue( 1477895624879L, 14));
        List<Measure> expectedMeasures8 = Arrays.asList(
                Measure.fromValue( 1477895624880L, 15),
                Measure.fromValue( 1477895624881L, 16));
        List<Measure> expectedMeasures9 = Arrays.asList(
                Measure.fromValue( 1477895624882L, 17),
                Measure.fromValue( 1477895624883L, 18));
        List<Measure> expectedMeasures10 = Arrays.asList(
                Measure.fromValue( 1477895624884L, 19),
                Measure.fromValue( 1477895624885L, 20));
        List<Measure> expectedMeasures11 = Arrays.asList(
                Measure.fromValue( 1477895624886L, 21),
                Measure.fromValue( 1477895624887L, 22));
        List<Measure> expectedMeasures12 = Arrays.asList(
                Measure.fromValue( 1477895624888L, 23),
                Measure.fromValue( 1477895624889L, 24));
        List<Measure> expectedMeasures13 = Arrays.asList(
                Measure.fromValue( 1477895624890L, 25),
                Measure.fromValue( 1477895624891L, 26));
        List<Measure> expectedMeasures14 = Arrays.asList(
                Measure.fromValue( 1477895624892L, 27),
                Measure.fromValue( 1477895624893L, 28));
        List<Measure> expectedMeasures15 = Arrays.asList(
                Measure.fromValue( 1477895624894L, 29),
                Measure.fromValue( 1477895624895L, 30));
        List<Measure> expectedMeasures16 = Arrays.asList(
                Measure.fromValue( 1477895624896L, 31),
                Measure.fromValue( 1477895624897L, 32));
        List<Measure> expectedMeasures17 = Arrays.asList(
                Measure.fromValue( 1477895624898L, 33),
                Measure.fromValue( 1477895624899L, 34));
        List<Measure> expectedMeasures18 = Arrays.asList(
                Measure.fromValue( 1477895624900L, 35),
                Measure.fromValue( 1477895624901L, 36));
        List<Measure> expectedMeasures19 = Arrays.asList(
                Measure.fromValue( 1477895624902L, 37),
                Measure.fromValue( 1477895624903L, 38));
        List<Measure> expectedMeasures20 = Arrays.asList(
                Measure.fromValue( 1477895624904L, 39),
                Measure.fromValue( 1477895624905L, 40));

        List<List<Measure>> ListMeasures = Arrays.asList(expectedMeasures1,expectedMeasures2,expectedMeasures3,
                expectedMeasures4,expectedMeasures5,expectedMeasures6,expectedMeasures7,expectedMeasures8,
                expectedMeasures9,expectedMeasures10, expectedMeasures11, expectedMeasures12,
                expectedMeasures13, expectedMeasures14, expectedMeasures15, expectedMeasures16,
                expectedMeasures17, expectedMeasures18, expectedMeasures19, expectedMeasures20);
        GeneralInjectorCurrentVersion chunkInjector = new GeneralInjectorCurrentVersion();
        for (List<Measure> expectedMeasures : ListMeasures) {
            byte[] compressedMeasures = ProtoBufTimeSeriesCurrentSerializer.to(expectedMeasures);
            long start = expectedMeasures.get(0).getTimestamp();
            long end = expectedMeasures.get(expectedMeasures1.size() - 1).getTimestamp();
            Chunk chunk = ChunkBuilderHelper.fromCompressedPoints("maxDataPoints", compressedMeasures, start, end);
            chunkInjector.addChunk(chunk);
        }
        return chunkInjector;
    }

    //@TODO fixit
    //@Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test1(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test1/request.json",
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test1/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    //@TODO fixit
    //@Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test2(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test2/request.json",
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }


    //@TODO fixit
    //@Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test1WithRange(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test1WithRange/request.json",
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test1WithRange/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    //@TODO fixit
    //@Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test2WithRange(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test2WithRange/request.json",
                        "/http/grafana/hurence/query/shouldNotReturnMorePointThanMaxDatapoints/test2WithRange/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}
