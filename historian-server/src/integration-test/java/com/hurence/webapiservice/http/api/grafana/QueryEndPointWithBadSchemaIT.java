package com.hurence.webapiservice.http.api.grafana;

import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
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

//import io.vertx.ext.web.client.WebClient;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointWithBadSchemaIT extends QueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointWithBadSchemaIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createHistorianCollection(client, SolrExtension.SOLR_CONF_TEMPLATE_HISTORIAN_VERSION_0);
        injectChunksIntoSolr(client, vertx);
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

}
