package com.hurence.historian.solr.util;

import com.hurence.historian.modele.solr.HistorianCollections;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.unit5.extensions.SolrExtension;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SolrITHelper {

    private SolrITHelper() {
    }

    private static Logger LOGGER = LoggerFactory.getLogger(SolrITHelper.class);
    public static String COLLECTION_HISTORIAN = HistorianCollections.DEFAULT_COLLECTION_HISTORIAN;
    public static String COLLECTION_ANNOTATION = HistorianCollections.DEFAULT_COLLECTION_ANNOTATION;
    public static String COLLECTION_REPORT = HistorianCollections.DEFAULT_COLLECTION_REPORT;
    public static File SHARED_RESSOURCE_FILE = new File(SolrITHelper.class.getResource("/shared-resources").getFile());
    public static String MODIFY_COLLECTION_SCRIPT_PATH = "./modify-collection-schema.sh";
    public static String CREATE_CHUNK_COLLECTION_SCRIPT_PATH = "./create-historian-chunk-collection.sh";
    public static String CREATE_REPORT_COLLECTION_SCRIPT_PATH = "./create-historian-report-collection.sh";
    public static String CREATE_ANNOTATION_COLLECTION_SCRIPT_PATH = "./create-historian-annotation-collection.sh";

    public static void creatingAllCollections(SolrClient client, String solrUrl, String modelVersion) throws IOException, InterruptedException, SolrServerException {
        List<ProcessBuilder> processBuilderList = startProcessesToCreateCollections(solrUrl, modelVersion);
        List<Process> processList = new ArrayList<>();
        for (ProcessBuilder processBuilder : processBuilderList) {
            LOGGER.debug("working directory of processe is {}", processBuilder.directory());
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
        ProcessBuilder p = new ProcessBuilder("bash", MODIFY_COLLECTION_SCRIPT_PATH,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_HISTORIAN,
                "--new-field", fieldName)
                .directory(SHARED_RESSOURCE_FILE);
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
        return new ProcessBuilder("bash", CREATE_REPORT_COLLECTION_SCRIPT_PATH,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_REPORT,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion)
                .directory(SHARED_RESSOURCE_FILE);
    }

    @NotNull
    private static ProcessBuilder getAnnotationCollectionCreationProcessBuilder(String solrUrl, String modelVersion) {
        return new ProcessBuilder("bash", CREATE_ANNOTATION_COLLECTION_SCRIPT_PATH,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", COLLECTION_ANNOTATION,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion)
                .directory(SHARED_RESSOURCE_FILE);
    }

    @NotNull
    private static ProcessBuilder getChunkCollectionCreationProcessBuilder(String collectionName, String solrUrl, String modelVersion) {
        return new ProcessBuilder("bash", CREATE_CHUNK_COLLECTION_SCRIPT_PATH,
                "--solr-host", solrUrl + "/solr",
                "--solr-collection", collectionName,
                "--replication-factor", "1",
                "--num-shards", "2",
                "--model-version", modelVersion)
                .directory(SHARED_RESSOURCE_FILE);
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
