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

import com.hurence.webapiservice.http.HttpServerVerticle;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import static com.hurence.webapiservice.util.HistorianSolrITHelper.HISTORIAN_ADRESS;

public class HttpITHelper {
    private static Logger LOGGER = LoggerFactory.getLogger(HttpITHelper.class);
    public static int PORT = 8080;
    public static String HOST = "localhost";

    public static WebClient buildWebClient(Vertx vertx) {
        return WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost(HOST)
                .setDefaultPort(PORT));
    }

    public static JsonObject getDefaultHttpConf() {
        return new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, HttpITHelper.PORT)
                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, HttpITHelper.HOST)
                .put(HttpServerVerticle.CONFIG_MAX_CSV_POINTS_ALLOWED, 10000)
                .put(HttpServerVerticle.CONFIG_DEBUG_MODE, true);
    }

    public static Single<String> deployHttpVerticle(Vertx vertx) {
        DeploymentOptions httpOptions = getDeploymentOptions();
        return vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions)
                .map(id -> {
                    LOGGER.info("HttpServerVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static Single<String> deployHttpVerticle(Vertx vertx,
                                                    JsonObject customHttpConf) {
        DeploymentOptions httpOptions = getDeploymentOptions(customHttpConf);
        return vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions)
                .map(id -> {
                    LOGGER.info("HttpServerVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions().setConfig(getDefaultHttpConf());
    }

    public static DeploymentOptions getDeploymentOptions(JsonObject customHistorianConf) {
        return new DeploymentOptions().setConfig(getDefaultHttpConf().mergeIn(customHistorianConf));
    }


}
