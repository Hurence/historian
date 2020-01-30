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

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;

import static com.hurence.webapiservice.util.HistorianSolrITHelper.HISTORIAN_ADRESS;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public abstract class HttpWithHistorianSolrITHelper {
    private static Logger LOGGER = LoggerFactory.getLogger(HttpWithHistorianSolrITHelper.class);
    private static int PORT = 8080;

    private HttpWithHistorianSolrITHelper() {}

    public static void initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(
            SolrClient client, DockerComposeContainer container,
            Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        LOGGER.info("Initializing Historian solr");
        HistorianSolrITHelper.initHistorianSolr(client);
        LOGGER.info("Initializing Verticles");
        deployHttpAndHistorianVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    private static Single<String> deployHttpAndHistorianVerticle(DockerComposeContainer container, Vertx vertx) {
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, PORT)
                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
        DeploymentOptions httpOptions = new DeploymentOptions().setConfig(httpConf);

        return HistorianSolrITHelper.deployHistorienVerticle(container, vertx)
                .flatMap(id -> vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions))
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static void initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(
            SolrClient client, DockerComposeContainer container,
            Vertx vertx, VertxTestContext context,
            JsonObject historianConf) throws IOException, SolrServerException {
        LOGGER.info("Initializing Historian solr");
        HistorianSolrITHelper.initHistorianSolr(client);
        LOGGER.info("Initializing Verticles");
        deployHttpAndHistorianVerticle(container, vertx, historianConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    private static Single<String> deployHttpAndHistorianVerticle(DockerComposeContainer container, Vertx vertx, JsonObject historianConf) {
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, PORT)
                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
        DeploymentOptions httpOptions = new DeploymentOptions().setConfig(httpConf);

        return HistorianSolrITHelper.deployHistorienVerticle(container, vertx, historianConf)
                .flatMap(id -> vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions))
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

}
