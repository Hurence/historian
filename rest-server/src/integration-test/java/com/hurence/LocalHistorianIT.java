package com.hurence;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;

/**
 * This test can be used to run a two node solr cluster with initial data. It also Run the historian server
 * on localhost:8080 so you can query it. It serves for exemple to test the rest api manually and do some debugging !
 */
@ExtendWith({VertxExtension.class, SolrExtension.class})
@Disabled("This test is usefull to start historian we some data and test REST api in local.")
public class LocalHistorianIT {

    private static Logger LOGGER = LoggerFactory.getLogger(LocalHistorianIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
        initVerticles(container, vertx, context);
    }

    @AfterAll
    static void finish(Vertx vertx, VertxTestContext context) {
        LOGGER.info("closing vertx");
        vertx.close(context.completing());
    }


    public static void initSolr(DockerComposeContainer container) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
    }

    public static void initVerticles(DockerComposeContainer container, Vertx vertx, VertxTestContext context) {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion(
                Arrays.asList("temp_a", "temp_b", "maxDataPoints", "metric with spaces"),
                Arrays.asList(
                        Arrays.asList(
                                Measure.fromValue( 1477895624866L, 622),
                                Measure.fromValue( 1477916224866L, -3),
                                Measure.fromValue( 1477917224866L, 365)
                        ),
                        Arrays.asList(
                                Measure.fromValue( 1477895624866L, 861),
                                Measure.fromValue( 1477917224866L, 767)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                Measure.fromValue( 1477895624866L, 1),
                                Measure.fromValue( 1477895624867L, 1),
                                Measure.fromValue( 1477895624868L, 1),
                                Measure.fromValue( 1477895624869L, 1),
                                Measure.fromValue( 1477895624870L, 1),
                                Measure.fromValue( 1477895624871L, 1),
                                Measure.fromValue( 1477895624872L, 1),
                                Measure.fromValue( 1477895624873L, 1),
                                Measure.fromValue( 1477895624874L, 1),
                                Measure.fromValue( 1477895624875L, 1),
                                Measure.fromValue( 1477895624876L, 1),
                                Measure.fromValue( 1477895624877L, 1),
                                Measure.fromValue( 1477895624878L, 1),
                                Measure.fromValue( 1477895624879L, 1),
                                Measure.fromValue( 1477895624880L, 1),
                                Measure.fromValue( 1477895624881L, 1),
                                Measure.fromValue( 1477895624882L, 1),
                                Measure.fromValue( 1477895624883L, 1),
                                Measure.fromValue( 1477895624884L, 1),
                                Measure.fromValue( 1477895624885L, 1),
                                Measure.fromValue( 1477895624886L, 1),
                                Measure.fromValue( 1477895624887L, 1),
                                Measure.fromValue( 1477895624888L, 1),
                                Measure.fromValue( 1477895624889L, 1),
                                Measure.fromValue( 1477895624890L, 1),
                                Measure.fromValue( 1477895624891L, 1),
                                Measure.fromValue( 1477895624892L, 1),
                                Measure.fromValue( 1477895624893L, 1),
                                Measure.fromValue( 1477895624894L, 1),
                                Measure.fromValue( 1477895624895L, 1),
                                Measure.fromValue( 1477895624896L, 1),
                                Measure.fromValue( 1477895624897L, 1),
                                Measure.fromValue( 1477895624898L, 1),
                                Measure.fromValue( 1477895624899L, 1),
                                Measure.fromValue( 1477895624900L, 1),
                                Measure.fromValue( 1477895624901L, 1),
                                Measure.fromValue( 1477895624902L, 1),
                                Measure.fromValue( 1477895624903L, 1),
                                Measure.fromValue( 1477895624904L, 1),
                                Measure.fromValue( 1477895624905L, 1)
                        ),
                        Arrays.asList(
                                Measure.fromValue( 1477895624866L, 861),
                                Measure.fromValue( 1477917224866L, 767)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }


    /**
     * This test can be used to run a two node solr cluster with initial data. It also Run the historian server
     * on localhost:8080 so you can query it. It serves for exemple to test the rest api manually and do some debugging !
     */
    @Test
    @Disabled("This test is usefull to start historian we some data and test REST api in local.")
    public void testRunLocalHistorian() {
        while(true) {
            try {
                LOGGER.info("waiting 100000 ms");
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
