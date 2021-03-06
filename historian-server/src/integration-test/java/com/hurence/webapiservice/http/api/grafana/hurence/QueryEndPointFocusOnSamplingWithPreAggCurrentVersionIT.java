package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnSamplingWithPreAggCurrentVersionIT extends AbstractQueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusOnSamplingWithPreAggCurrentVersionIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
        injectChunksIntoSolr(client);
        initSolrAndVerticles(client, container, vertx, context);
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static void initSolrAndVerticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) {
        JsonObject historianConf = buildHistorianConf();
        HttpWithHistorianSolrITHelper
                .deployHttpAndCustomHistorianVerticle(container, vertx, historianConf)
                .subscribe(id -> {
                            context.completeNow();
                        },
                        t -> context.failNow(t));
    }

    public static JsonObject buildHistorianConf() {
        return new JsonObject()
                    //10 so if more than 5 chunk (of size 2) returned we should sample
                    //with pre aggs
                    .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_POINT, 10L)
                    .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 10000L);
    }

    public static SolrInjector buildInjector() {
        GeneralInjectorCurrentVersion injector = new GeneralInjectorCurrentVersion();
        List<List<Measure>> pointsByChunk10Chunks = Arrays.asList(
                Arrays.asList(
                        Measure.fromValue( 1L, 1.0),
                        Measure.fromValue( 2L, 1.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 3L, 2.0),
                        Measure.fromValue( 4L, 2.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 5L, 3.0),
                        Measure.fromValue( 6L, 3.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 7L, 4.0),
                        Measure.fromValue( 8L, 4.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 9L, 5.0),
                        Measure.fromValue( 10L, 5.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 11L, 6.0),
                        Measure.fromValue( 12L, 6.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 13L, 7.0),
                        Measure.fromValue( 14L, 7.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 15L, 8.0),
                        Measure.fromValue( 16L, 8.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 17L, 9.0),
                        Measure.fromValue( 18L, 9.0)
                ),
                Arrays.asList(
                        Measure.fromValue( 19L, 10.0),
                        Measure.fromValue( 20L, 10.0)
                )
        );
        List<Chunk> metric_10_chunk = buildChunks(
                "metric_10_chunk",
                pointsByChunk10Chunks);
        injector.addChunks(metric_10_chunk);
        List<Chunk> metric_9_chunk = buildChunks(
                "metric_9_chunk",
                pointsByChunk10Chunks).stream().limit(9).collect(Collectors.toList());
        injector.addChunks(metric_9_chunk);
        List<Chunk> metric_7_chunk = buildChunks(
                "metric_7_chunk",
                pointsByChunk10Chunks).stream().limit(7).collect(Collectors.toList());
        injector.addChunks(metric_7_chunk);
        List<Chunk> metric_5_chunk = buildChunks(
                "metric_5_chunk",
                pointsByChunk10Chunks).stream().limit(5).collect(Collectors.toList());
        injector.addChunks(metric_5_chunk);
        List<Chunk> metric_1_chunk_of_20_points = buildChunks(
                "metric_1_chunk_of_20_points",
                Arrays.asList(
                        pointsByChunk10Chunks.stream().flatMap(List::stream).collect(Collectors.toList())
                )
        );
        injector.addChunks(metric_1_chunk_of_20_points);
        return injector;
    }

    private static List<Chunk> buildChunks(String metric_name, List<List<Measure>> pointsByChunk) {
        return ChunkBuilderHelper.fromGroupOfMeasures(metric_name, pointsByChunk);
    }

}
