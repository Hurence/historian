package com.hurence.webapiservice.http.api.grafana.simplejson;

import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.Version0SolrInjectorMultipleMetricSpecificPoints;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
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


public class QueryEndPointVersion0IT extends AbstractQueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointVersion0IT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolrAndVerticles(client, container, vertx, context);
        injectChunksIntoSolr(client, vertx);
    }

    public static void injectChunksIntoSolr(SolrClient client, Vertx vertx) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new Version0SolrInjectorMultipleMetricSpecificPoints(
                Arrays.asList("temp_a", "temp_b", "maxDataPoints", "metric with spaces"),
                Arrays.asList(
                        Arrays.asList(
                                new PointImpl( 1477895624866L, 622),
                                new PointImpl( 1477916224866L, -3),
                                new PointImpl( 1477917224866L, 365)
                        ),
                        Arrays.asList(
                                new PointImpl( 1477895624866L, 861),
                                new PointImpl( 1477917224866L, 767)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                new PointImpl( 1477895624866L, 1),
                                new PointImpl( 1477895624867L, 1),
                                new PointImpl( 1477895624868L, 1),
                                new PointImpl( 1477895624869L, 1),
                                new PointImpl( 1477895624870L, 1),
                                new PointImpl( 1477895624871L, 1),
                                new PointImpl( 1477895624872L, 1),
                                new PointImpl( 1477895624873L, 1),
                                new PointImpl( 1477895624874L, 1),
                                new PointImpl( 1477895624875L, 1),
                                new PointImpl( 1477895624876L, 1),
                                new PointImpl( 1477895624877L, 1),
                                new PointImpl( 1477895624878L, 1),
                                new PointImpl( 1477895624879L, 1),
                                new PointImpl( 1477895624880L, 1),
                                new PointImpl( 1477895624881L, 1),
                                new PointImpl( 1477895624882L, 1),
                                new PointImpl( 1477895624883L, 1),
                                new PointImpl( 1477895624884L, 1),
                                new PointImpl( 1477895624885L, 1),
                                new PointImpl( 1477895624886L, 1),
                                new PointImpl( 1477895624887L, 1),
                                new PointImpl( 1477895624888L, 1),
                                new PointImpl( 1477895624889L, 1),
                                new PointImpl( 1477895624890L, 1),
                                new PointImpl( 1477895624891L, 1),
                                new PointImpl( 1477895624892L, 1),
                                new PointImpl( 1477895624893L, 1),
                                new PointImpl( 1477895624894L, 1),
                                new PointImpl( 1477895624895L, 1),
                                new PointImpl( 1477895624896L, 1),
                                new PointImpl( 1477895624897L, 1),
                                new PointImpl( 1477895624898L, 1),
                                new PointImpl( 1477895624899L, 1),
                                new PointImpl( 1477895624900L, 1),
                                new PointImpl( 1477895624901L, 1),
                                new PointImpl( 1477895624902L, 1),
                                new PointImpl( 1477895624903L, 1),
                                new PointImpl( 1477895624904L, 1),
                                new PointImpl( 1477895624905L, 1)
                        ),
                        Arrays.asList(
                                new PointImpl( 1477895624866L, 861),
                                new PointImpl( 1477917224866L, 767)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static void initSolrAndVerticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        HttpWithHistorianSolrITHelper
                .initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
    }

}
