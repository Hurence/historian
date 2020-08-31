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
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.reactivex.Single;
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

@ExtendWith({VertxExtension.class, SolrExtension.class})
public abstract class HttpWithHistorianSolrITHelper {
    private static Logger LOGGER = LoggerFactory.getLogger(HttpWithHistorianSolrITHelper.class);

    private HttpWithHistorianSolrITHelper() {}

    public static void initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(
            SolrClient client, DockerComposeContainer container,
            Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx,
                context, new JsonObject());
    }

    public static Single<String> deployHttpAndHistorianVerticle(DockerComposeContainer container, Vertx vertx) {
        return deployHttpAndCustomHistorianVerticle(container, vertx, new JsonObject());
    }

    public static void initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(
            SolrClient client, DockerComposeContainer container,
            Vertx vertx, VertxTestContext context,
            JsonObject historianConf) throws IOException, SolrServerException, InterruptedException {
        LOGGER.info("Initializing Historian chunk collection");
        HistorianSolrITHelper.createChunkCollection(client, container, SchemaVersion.VERSION_1);
        LOGGER.info("Initializing Historian annotation collection");
        HistorianSolrITHelper.createAnnotationCollection(client, container, SchemaVersion.VERSION_1);
        LOGGER.info("Initializing Verticles");
        deployHttpAndCustomHistorianVerticle(container, vertx, historianConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    public static Single<String> deployCustomHttpAndCustomHistorianVerticle(DockerComposeContainer container, Vertx vertx,
                                                                            JsonObject historianConf, JsonObject httpConf) {
        return HistorianSolrITHelper.deployHistorianVerticle(container, vertx, historianConf)
                .flatMap(id -> HttpITHelper.deployHttpVerticle(vertx, httpConf))
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static Single<String> deployHttpAndCustomHistorianVerticle(DockerComposeContainer container, Vertx vertx,
                                                                      JsonObject historianConf) {
        return deployCustomHttpAndCustomHistorianVerticle(container, vertx, historianConf, new JsonObject());
    }

    public static Single<String> deployCustomHttpAndHistorianVerticle(DockerComposeContainer container, Vertx vertx,
                                                                      JsonObject httpConf) {
        return deployCustomHttpAndCustomHistorianVerticle(container, vertx, new JsonObject(), httpConf);
    }

    public static Single<String> deployCustomHttpAndHistorianVerticle(DockerComposeContainer container, Vertx vertx, int maxLimitFromConfig) {
        return deployCustomHttpAndCustomHistorianVerticle(container, vertx, new JsonObject(),
                new JsonObject().put(HttpServerVerticle.CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED, maxLimitFromConfig)
        );
    }

}
