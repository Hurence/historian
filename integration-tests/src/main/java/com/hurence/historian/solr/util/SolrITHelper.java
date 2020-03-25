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

package com.hurence.historian.solr.util;

import io.vertx.core.json.JsonArray;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.hurence.unit5.extensions.SolrExtension.SOLR_CONF_TEMPLATE_ANNOTATION;
import static com.hurence.unit5.extensions.SolrExtension.SOLR_CONF_TEMPLATE_HISTORIAN;

public class SolrITHelper {

    private SolrITHelper() {}

    private static Logger LOGGER = LoggerFactory.getLogger(SolrITHelper.class);
    public static String COLLECTION_HISTORIAN = "historian";
    public static String COLLECTION_ANNOTATION = "annotation";
//    public static String COLLECTION_REPORT = "report";

    public static void initHistorianSolr(SolrClient client) throws IOException, SolrServerException {
        LOGGER.debug("creating collection {}", COLLECTION_HISTORIAN);
        createHistorianCollection(client);
        LOGGER.debug("creating collection {}", COLLECTION_ANNOTATION);
        createAnnotationCollection(client);
        LOGGER.debug("verify collections {} and {} exist and are ready", COLLECTION_HISTORIAN, COLLECTION_ANNOTATION);
        checkCollectionsHasBeenCreated(client);
        LOGGER.debug("printing conf {} and {}", COLLECTION_HISTORIAN, COLLECTION_ANNOTATION);
        checkCollectionsSchema(client);
    }

    private static void checkCollectionsSchema(SolrClient client) throws IOException, SolrServerException {
        checkHistorianSchema(client);
        checkAnnotationSchema(client);
    }

    private static void checkCollectionsHasBeenCreated(SolrClient client) throws IOException, SolrServerException {
        checkHistorianCollectionHasBeenCreated(client);
        checkAnnotationCollectionHasBeenCreated(client);
    }

    private static void checkHistorianSchema(SolrClient client) throws SolrServerException, IOException {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION_HISTORIAN);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
        LOGGER.debug(COLLECTION_HISTORIAN + "schema is {}", new JsonArray(schema).encodePrettily());
    }

    private static void checkAnnotationSchema(SolrClient client) throws SolrServerException, IOException {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION_ANNOTATION);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
        LOGGER.debug(COLLECTION_ANNOTATION + "schema is {}", new JsonArray(schema).encodePrettily());
    }

    private static void checkHistorianCollectionHasBeenCreated(SolrClient client) throws SolrServerException, IOException {
        final SolrRequest request = CollectionAdminRequest.collectionStatus(COLLECTION_HISTORIAN);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", COLLECTION_HISTORIAN));
        }
        LOGGER.info("collection {} is up and running !", COLLECTION_HISTORIAN);
    }

    private static void checkAnnotationCollectionHasBeenCreated(SolrClient client) throws SolrServerException, IOException {
        final SolrRequest request = CollectionAdminRequest.collectionStatus(COLLECTION_ANNOTATION);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", COLLECTION_ANNOTATION));
        }
        LOGGER.info("collection {} is up and running !", COLLECTION_ANNOTATION);
    }

    private static void createHistorianCollection(SolrClient client) throws SolrServerException, IOException {
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION_HISTORIAN, SOLR_CONF_TEMPLATE_HISTORIAN, 2, 1);
        client.request(createrequest);
    }
    private static void createAnnotationCollection(SolrClient client) throws SolrServerException, IOException {
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION_ANNOTATION, SOLR_CONF_TEMPLATE_ANNOTATION, 2, 1);
        client.request(createrequest);
    }
}
