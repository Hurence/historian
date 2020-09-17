package com.hurence.webapiservice.http.api.grafana.simplejson;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.AbstractSolrInjectorChunkCurrentVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.modele.points.Point;
import com.hurence.timeseries.modele.points.PointImpl;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class QueryEndPointFocusOnSamplingWithPreAggIT extends AbstractQueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusOnSamplingWithPreAggIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        initVerticles(container, vertx, context);
        injectChunksIntoSolr(client);
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static void initVerticles(DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        JsonObject historianConf = buildHistorianConf();
        HttpWithHistorianSolrITHelper.deployHttpAndCustomHistorianVerticle(container, vertx, historianConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }


    public static void initSolr(DockerComposeContainer container) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
    }

    public static JsonObject buildHistorianConf() {
        return new JsonObject()
                    //10 so if more than 5 chunk (of size 2) returned we should sample
                    //with pre aggs
                    .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_POINT, 10L)
                    .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 10000L);
    }

    public static SolrInjector buildInjector() {
        List<List<Point>> pointsByChunk10Chunks = Arrays.asList(
                Arrays.asList(
                        new PointImpl( 1L, 1.0),
                        new PointImpl( 2L, 1.0)
                ),
                Arrays.asList(
                        new PointImpl( 3L, 2.0),
                        new PointImpl( 4L, 2.0)
                ),
                Arrays.asList(
                        new PointImpl( 5L, 3.0),
                        new PointImpl( 6L, 3.0)
                ),
                Arrays.asList(
                        new PointImpl( 7L, 4.0),
                        new PointImpl( 8L, 4.0)
                ),
                Arrays.asList(
                        new PointImpl( 9L, 5.0),
                        new PointImpl( 10L, 5.0)
                ),
                Arrays.asList(
                        new PointImpl( 11L, 6.0),
                        new PointImpl( 12L, 6.0)
                ),
                Arrays.asList(
                        new PointImpl( 13L, 7.0),
                        new PointImpl( 14L, 7.0)
                ),
                Arrays.asList(
                        new PointImpl( 15L, 8.0),
                        new PointImpl( 16L, 8.0)
                ),
                Arrays.asList(
                        new PointImpl( 17L, 9.0),
                        new PointImpl( 18L, 9.0)
                ),
                Arrays.asList(
                        new PointImpl( 19L, 10.0),
                        new PointImpl( 20L, 10.0)
                )
        );
        AbstractSolrInjectorChunkCurrentVersion injector10chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_10_chunk", pointsByChunk10Chunks);
        AbstractSolrInjectorChunkCurrentVersion injector9chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_9_chunk", pointsByChunk10Chunks.stream().limit(9).collect(Collectors.toList()));
        AbstractSolrInjectorChunkCurrentVersion injector7chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_7_chunk", pointsByChunk10Chunks.stream().limit(7).collect(Collectors.toList()));
        AbstractSolrInjectorChunkCurrentVersion injector5chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_5_chunk", pointsByChunk10Chunks.stream().limit(5).collect(Collectors.toList()));
        AbstractSolrInjectorChunkCurrentVersion injector1chunkOf20Point = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_1_chunk_of_20_points",
                Arrays.asList(
                        pointsByChunk10Chunks.stream().flatMap(List::stream).collect(Collectors.toList())
                )
        );
        injector10chunk.addChunk(injector9chunk);
        injector10chunk.addChunk(injector7chunk);
        injector10chunk.addChunk(injector5chunk);
        injector10chunk.addChunk(injector1chunkOf20Point);
        return injector10chunk;
    }
}
