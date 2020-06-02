package com.hurence.historian.solr.util;

import com.hurence.historian.modele.HistorianCollections;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.unit5.extensions.SolrExtension;
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
import org.testcontainers.containers.DockerComposeContainer;

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

    public static void addCodeInstallAndSensor(DockerComposeContainer container) throws IOException, InterruptedException, SolrServerException {
        addCodeInstallAndSensor(SolrExtension.getSolr1Url(container));
    }

    public static void addCodeInstallAndSensor(String solrUrl) throws IOException, InterruptedException, SolrServerException {
        SolrITHelper.addFieldToChunkSchema(solrUrl, TimeSeriesRecord.CODE_INSTALL);
        SolrITHelper.addFieldToChunkSchema(solrUrl, TimeSeriesRecord.SENSOR);
    }

    public static void addFieldToChunkSchema(DockerComposeContainer container, String fieldName) throws IOException, InterruptedException, SolrServerException {
        SolrITHelper.addFieldToChunkSchema(SolrExtension.getSolr1Url(container), fieldName);
    }

    public static void addFieldToChunkSchema(String solrUrl, String fieldName) throws IOException, InterruptedException, SolrServerException {

        String modifyCollectionScriptPath = SolrITHelper.class.getResource("/shared-resources/modify-collection-schema.sh").getFile();
        ProcessBuilder p = new ProcessBuilder("bash", modifyCollectionScriptPath,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_HISTORIAN,
                "--new-field", fieldName);
        LOGGER.info("start process to add field {} in collection {}", fieldName, COLLECTION_HISTORIAN);
        Process process = p.start();
        waitProcessFinishedAndPrintResult(process);
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
        ProcessBuilder pbChunkCollection = getChunkCollectionCreationProcessBuilder(COLLECTION_HISTORIAN, solrUrl, modelVersion);
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
    private static ProcessBuilder getChunkCollectionCreationProcessBuilder(String collectionName, String solrUrl, String modelVersion) {
        String chunkCollectionScriptPath = SolrITHelper.class.getResource("/shared-resources/create-historian-chunk-collection.sh").getFile();
        return new ProcessBuilder("bash", chunkCollectionScriptPath,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", collectionName,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion);
    }

    public static void createReportCollection(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        Process process = getReportCollectionCreationProcessBuilder(solrUrl, modelVersion).start();
        waitProcessFinishedAndPrintResult(process);
    }

    private static void waitProcessFinishedAndPrintResult(Process process) throws IOException, InterruptedException {
        int exitCode = process.waitFor();
        LOGGER.info("standard output :\n\n{}", convertInputStreamToString(process.getInputStream()));
        LOGGER.info("process exited with code {}", exitCode);
        if (exitCode != 0) {
            LOGGER.error("error output :\n\n{}", convertInputStreamToString(process.getErrorStream()));
        }
    }

    public static void createChunkCollection(String collectionName, String solrUrl, SchemaVersion modelVersion) throws IOException, InterruptedException, SolrServerException {
        createChunkCollection(collectionName, solrUrl, modelVersion.toString());
    }

    public static void createChunkCollection(String collectionName, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        Process process = getChunkCollectionCreationProcessBuilder(collectionName, solrUrl, modelVersion).start();
        waitProcessFinishedAndPrintResult(process);
    }

    @Deprecated
    public static void createChunkCollection(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        Process process = getChunkCollectionCreationProcessBuilder(COLLECTION_HISTORIAN, solrUrl, modelVersion).start();
        waitProcessFinishedAndPrintResult(process);
    }

    public static void createAnnotationCollection(SolrClient client, String solrUrl, SchemaVersion modelVersion) throws IOException, InterruptedException, SolrServerException {
        createAnnotationCollection(client, solrUrl, modelVersion.toString());
    }

    @Deprecated
    public static void createAnnotationCollection(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        Process process = getAnnotationCollectionCreationProcessBuilder(solrUrl, modelVersion).start();
        waitProcessFinishedAndPrintResult(process);
    }

}
