/*
 *  Copyright (c) 2017 Red Hat, Inc. and/or its affiliates.
 *  Copyright (c) 2017 INSA Lyon, CITI Laboratory.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.webapiservice.util;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
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

import static com.hurence.unit5.extensions.SolrExtension.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianSolrITHelper {

    private HistorianSolrITHelper() {}

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianSolrITHelper.class);
    public static String COLLECTION_HISTORIAN = SolrITHelper.COLLECTION_HISTORIAN;
    public static String COLLECTION_ANNOTATION = SolrITHelper.COLLECTION_ANNOTATION;
    public static String HISTORIAN_ADRESS = "historian_service";

    public static void createChunkCollection(SolrClient client, DockerComposeContainer container, SchemaVersion version) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(client, SolrExtension.getSolr1Url(container), version.toString());
    }

    @BeforeAll
    public static void initHistorianAndDeployVerticle(SolrClient client,
                                                      DockerComposeContainer container,
                                                      Vertx vertx, VertxTestContext context,
                                                      SchemaVersion version) throws IOException, SolrServerException, InterruptedException {
        createChunkCollection(client, container, version);
        LOGGER.info("Initializing Verticles");
        deployHistorienVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    public static Single<String> deployHistorienVerticle(DockerComposeContainer container, Vertx vertx) {
        DeploymentOptions historianOptions = getDeploymentOptions(container);
        return vertx.rxDeployVerticle(new HistorianVerticle(), historianOptions)
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static Single<String> deployHistorienVerticle(DockerComposeContainer container,
                                                         Vertx vertx,
                                                         JsonObject customHistorianConf) {
        DeploymentOptions historianOptions = getDeploymentOptions(container, customHistorianConf);
        return vertx.rxDeployVerticle(new HistorianVerticle(), historianOptions)
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static DeploymentOptions getDeploymentOptions(DockerComposeContainer container) {
        JsonObject historianConf = getHistorianConf(container);
        return new DeploymentOptions().setConfig(historianConf);
    }

    private static JsonObject getHistorianConf(DockerComposeContainer container) {
        String zkUrl = container.getServiceHost(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT)
                + ":" +
                container.getServicePort(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT);
        String slr1Url = container.getServiceHost(SOLR1_SERVICE_NAME, SOLR_1_PORT)
                + ":" +
                container.getServicePort(SOLR1_SERVICE_NAME, SOLR_1_PORT);

        JsonObject solrConf = new JsonObject()
                .put(HistorianVerticle.CONFIG_SOLR_CHUNK_COLLECTION, COLLECTION_HISTORIAN)
                .put(HistorianVerticle.CONFIG_SOLR_ANNOTATION_COLLECTION, COLLECTION_ANNOTATION)
                .put(HistorianVerticle.CONFIG_SOLR_USE_ZOOKEEPER, true)
                .put(HistorianVerticle.CONFIG_SOLR_ZOOKEEPER_URLS, new JsonArray().add(zkUrl))
                .put(HistorianVerticle.CONFIG_SOLR_STREAM_ENDPOINT, "http://" + slr1Url + "/solr/" + COLLECTION_HISTORIAN)
                .put(HistorianVerticle.MAX_NUMBER_OF_TARGET_RETURNED, 100);
        JsonObject grafana = new JsonObject()
                .put(HistorianVerticle.CONFIG_GRAFANA_HISTORAIN, new JsonObject()
                        .put(HistorianVerticle.CONFIG_SEARCH_HISTORAIN, new JsonObject()
                                .put(HistorianVerticle.CONFIG_DEFAULT_SIZE_HISTORAIN, 100)));
        return new JsonObject()
                .put(HistorianVerticle.CONFIG_ROOT_SOLR, solrConf)
                .put(HistorianVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HistorianVerticle.CONFIG_API_HISTORAIN, grafana);
    }

    public static DeploymentOptions getDeploymentOptions(DockerComposeContainer container,
                                                         JsonObject customHistorianConf) {
        JsonObject historianConf = getHistorianConf(container);
        return new DeploymentOptions().setConfig(historianConf.mergeIn(customHistorianConf));
    }

    public static void createAnnotationCollection(SolrClient client, DockerComposeContainer container, SchemaVersion version) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createAnnotationCollection(client, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0);
    }
}
