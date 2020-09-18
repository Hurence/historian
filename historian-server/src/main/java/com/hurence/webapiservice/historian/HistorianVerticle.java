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

package com.hurence.webapiservice.historian;

import com.hurence.webapiservice.historian.models.HistorianVerticleConf;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HistorianVerticle extends AbstractVerticle {

  private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticle.class);

  //root conf
  public static final String CONFIG_HISTORIAN_ADDRESS = "address";
  public static final String CONFIG_LIMIT_NUMBER_OF_POINT = "limit_number_of_point_before_using_pre_agg";
  public static final String CONFIG_LIMIT_NUMBER_OF_CHUNK = "limit_number_of_chunks_before_using_solr_partition";
  public static final String CONFIG_API_HISTORAIN = "api";
  public static final String CONFIG_SEARCH_HISTORAIN = "search";
  public static final String CONFIG_DEFAULT_SIZE_HISTORAIN = "default_size";
  public static final String CONFIG_GRAFANA_HISTORAIN = "grafana";
  public static final String CONFIG_ROOT_SOLR = "solr";
  public static final String CONFIG_SCHEMA_VERSION = "schema_version";

  //solr conf
  public static final String CONFIG_SOLR_URLS = "urls";
  public static final String CONFIG_SOLR_USE_ZOOKEEPER = "use_zookeeper";
  public static final String CONFIG_SOLR_ZOOKEEPER_ROOT = "zookeeper_chroot";//see zookeeper documentation about chroot
  public static final String CONFIG_SOLR_ZOOKEEPER_URLS = "zookeeper_urls";
  public static final String CONFIG_SOLR_CONNECTION_TIMEOUT = "connection_timeout";
  public static final String CONFIG_SOLR_SOCKET_TIMEOUT = "socket_timeout";
  public static final String CONFIG_SOLR_CHUNK_COLLECTION = "chunk_collection";
  public static final String CONFIG_SOLR_ANNOTATION_COLLECTION = "annotation_collection";
  public static final String CONFIG_SOLR_STREAM_ENDPOINT = "stream_url";
  public static final String CONFIG_SOLR_SLEEP_BETWEEEN_TRY = "sleep_milli_between_connection_attempt";
  public static final String CONFIG_SOLR_NUMBER_CONNECTION_ATTEMPT = "number_of_connection_attempt";
  public static final String MAX_NUMBER_OF_TARGET_RETURNED = "max_number_of_target_returned";


  private HistorianVerticleConf conf;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    this.conf = parseConfig(config());
    LOGGER.info("deploying {} verticle with config : {}", HistorianVerticle.class.getSimpleName(), this.conf);

    HistorianService.create(vertx, conf.getHistorianConf(), ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder.setAddress(conf.getHistorianServiceAddress())
                .register(HistorianService.class, ready.result());
        LOGGER.info("{} deployed on address : '{}'", HistorianService.class.getSimpleName(), conf.getHistorianServiceAddress());
        promise.complete();
      } else {
        LOGGER.error("Initialisation of historian verticle failed", ready.cause());
        promise.fail(ready.cause());
      }
    });
  }

  public HistorianVerticleConf parseConfig(JsonObject config) {
    return new HistorianVerticleConf(config);
  }


  @Override
  public void stop(Promise<Void> promise) throws Exception {
    if (this.getClient() != null) {
      getClient().close();
    }
    promise.complete();
  }

  private SolrClient getClient() {
    if (this.conf != null && this.conf.getHistorianConf() != null) {
      return this.conf.getHistorianConf().client;
    }
    return null;
  }
}
