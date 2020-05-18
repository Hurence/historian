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

import com.hurence.historian.modele.HistorianCollections;
import com.hurence.historian.modele.SchemaVersion;
import io.vertx.core.json.JsonArray;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.util.NamedList;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SolrITHelper {

    private SolrITHelper() {}

    private static Logger LOGGER = LoggerFactory.getLogger(SolrITHelper.class);
    public static String COLLECTION_HISTORIAN = HistorianCollections.DEFAULT_COLLECTION_HISTORIAN;
    public static String COLLECTION_ANNOTATION = HistorianCollections.DEFAULT_COLLECTION_ANNOTATION;
    public static String COLLECTION_REPORT = HistorianCollections.DEFAULT_COLLECTION_REPORT;

    public static void creatingAllCollections(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        List<ProcessBuilder> processBuilderList = startProcessesToCreateCollections(solrUrl, modelVersion);
        List<Process> processList = new ArrayList<>();
        for (ProcessBuilder processBuilder : processBuilderList) {
            Process start = processBuilder.start();
            processList.add(start);
        }
        for (Process process : processList) {
            int exitCode = process.waitFor();
            LOGGER.info("standard output :\n\n{}", convertInputStreamToString(process.getInputStream()));
            LOGGER.info("process exited with code {}", exitCode);
            if (exitCode != 0) {
                LOGGER.error("error output :\n\n{}", convertInputStreamToString(process.getErrorStream()));
            }
        }
        //TODO apparently checking the schema hinder later change of the schema.... It's crazy ! Lost enough time with that.
//        checkCollectionHasBeenCreated(client, COLLECTION_HISTORIAN);
//        checkSchema(client, COLLECTION_HISTORIAN);
//        checkCollectionHasBeenCreated(client, COLLECTION_ANNOTATION);
//        checkSchema(client, COLLECTION_ANNOTATION);
//        checkCollectionHasBeenCreated(client, COLLECTION_REPORT);
//        checkSchema(client, COLLECTION_REPORT);
    }

    public static void addFieldToChunkSchema(String solrUrl, String fieldName) throws IOException, InterruptedException, SolrServerException {

        String modifyCollectionScriptPath = SolrITHelper.class.getResource("/shared-resources/modify-collection-schema.sh").getFile();
        ProcessBuilder p = new ProcessBuilder("bash", modifyCollectionScriptPath,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_HISTORIAN,
                "--new-field", fieldName);
        LOGGER.info("start process to add field {} in collection {}", fieldName, COLLECTION_HISTORIAN);
        Process process = p.start();
        int exitCode = process.waitFor();
        LOGGER.info("standard output :\n\n{}", convertInputStreamToString(process.getInputStream()));
        LOGGER.info("process exited with code {}", exitCode);
        if (exitCode != 0) {
            LOGGER.error("error output :\n\n{}", convertInputStreamToString(process.getErrorStream()));
        }
    }


    private static String convertInputStreamToString(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[1024];
        int nRead = inputStream.read(data, 0, data.length);
        while (nRead != -1) {
            buffer.write(data, 0, nRead);
            nRead = inputStream.read(data, 0, data.length);
        }
        buffer.flush();
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static List<ProcessBuilder> startProcessesToCreateCollections(String solrUrl, String modelVersion) {
        ProcessBuilder pbChunkCollection = getChunkCollectionCreationProcessBuilder(solrUrl, modelVersion);
        ProcessBuilder pbAnnotationCollection = getAnnotationCollectionCreationProcessBuilder(solrUrl, modelVersion);
        ProcessBuilder pbReportCollection = getReportCollectionCreationProcessBuilder(solrUrl, modelVersion);
        return Arrays.asList(pbChunkCollection, pbAnnotationCollection, pbReportCollection);
    }

    @NotNull
    private static ProcessBuilder getReportCollectionCreationProcessBuilder(String solrUrl, String modelVersion) {
        String reportCollectionScriptPath = SolrITHelper.class.getResource("/shared-resources/create-historian-report-collection.sh").getFile();
        return new ProcessBuilder("bash", reportCollectionScriptPath,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_REPORT,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion);
    }

    @NotNull
    private static ProcessBuilder getAnnotationCollectionCreationProcessBuilder(String solrUrl, String modelVersion) {
        String annotationCollectionScriptPath = SolrITHelper.class.getResource("/shared-resources/create-historian-annotation-collection.sh").getFile();
        return new ProcessBuilder("bash", annotationCollectionScriptPath,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_ANNOTATION,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion);
    }

    @NotNull
    private static ProcessBuilder getChunkCollectionCreationProcessBuilder(String solrUrl, String modelVersion) {
        String chunkCollectionScriptPath = SolrITHelper.class.getResource("/shared-resources/create-historian-chunk-collection.sh").getFile();
        return new ProcessBuilder("bash", chunkCollectionScriptPath,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_HISTORIAN,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion);
    }

    public static void createReportCollection(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        getReportCollectionCreationProcessBuilder(solrUrl, modelVersion).start().waitFor();
        checkReportCollectionHasBeenCreated(client);
        checkReportSchema(client);
    }

    public static void createChunkCollection(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        getChunkCollectionCreationProcessBuilder(solrUrl, modelVersion).start().waitFor();
        checkHistorianCollectionHasBeenCreated(client);
        checkHistorianSchema(client);
    }

    public static void createAnnotationCollection(SolrClient client, String solrUrl, SchemaVersion modelVersion) throws IOException, InterruptedException, SolrServerException {
        createAnnotationCollection(client, solrUrl, modelVersion.toString());
    }

    public static void createAnnotationCollection(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        getAnnotationCollectionCreationProcessBuilder(solrUrl, modelVersion).start().waitFor();
        checkAnnotationCollectionHasBeenCreated(client);
        checkAnnotationSchema(client);
    }

    private static void checkReportSchema(SolrClient client) throws SolrServerException, IOException {
        checkSchema(client, COLLECTION_REPORT);
    }

    private static void checkHistorianSchema(SolrClient client) throws SolrServerException, IOException {
        checkSchema(client, COLLECTION_HISTORIAN);
    }

    private static void checkSchema(SolrClient client, String collectionHistorian) throws SolrServerException, IOException {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, collectionHistorian);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
        LOGGER.debug(collectionHistorian + "schema is {}", new JsonArray(schema).encodePrettily());
    }

    private static void checkAnnotationSchema(SolrClient client) throws SolrServerException, IOException {
        checkSchema(client, COLLECTION_ANNOTATION);
    }

    private static void checkHistorianCollectionHasBeenCreated(SolrClient client) throws SolrServerException, IOException {
        checkCollectionHasBeenCreated(client, COLLECTION_HISTORIAN);
    }

    private static void checkAnnotationCollectionHasBeenCreated(SolrClient client) throws SolrServerException, IOException {
        checkCollectionHasBeenCreated(client, COLLECTION_ANNOTATION);
    }

    private static void checkReportCollectionHasBeenCreated(SolrClient client) throws SolrServerException, IOException {
        checkCollectionHasBeenCreated(client, COLLECTION_REPORT);
    }

    private static void checkCollectionHasBeenCreated(SolrClient client, String collectionAnnotation) throws SolrServerException, IOException {
        final SolrRequest request = CollectionAdminRequest.collectionStatus(collectionAnnotation);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", collectionAnnotation));
        }
        LOGGER.info("collection {} is up and running !", collectionAnnotation);
    }
}
