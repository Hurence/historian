package com.hurence.historian.solr.util;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.modele.solr.HistorianCollections;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.compaction.BinaryEncodingUtils;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.jetbrains.annotations.NotNull;
import org.noggit.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.converter.SolrDocumentReader.fromSolrDocument;
import static com.hurence.timeseries.model.HistorianChunkCollectionFieldsVersionCurrent.*;
import static org.junit.jupiter.api.Assertions.*;

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
        SolrITHelper.addFieldToChunkSchema(solrUrl, HistorianChunkCollectionFieldsVersionEVOA0.CODE_INSTALL);
        SolrITHelper.addFieldToChunkSchema(solrUrl, HistorianChunkCollectionFieldsVersionEVOA0.SENSOR);
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

    /**
     * Read Chunks stored in Solr as a map of chunk id -> chunk
     * @param solrClient
     * @param collection
     * @return
     */
    public static Map<String, Chunk> getSolrChunks(SolrClient solrClient, String collection) {

        Map<String, Chunk> chunks = new HashMap<String, Chunk>();

        SolrDocumentList solrDocumentList = getSolrDocs(solrClient, collection);

        for (SolrDocument solrDocument : solrDocumentList) {
            Chunk chunk = fromSolrDocument(solrDocument);
            chunks.put(chunk.getId(), chunk);
        }

        return chunks;
    }

    /**
     * Get solr docs of a collection
     * @param solrClient
     * @param collection
     * @return
     */
    public static SolrDocumentList getSolrDocs(SolrClient solrClient, String collection) {
        SolrParams solrQuery = new SolrQuery("*:*").setRows(1000);
        QueryResponse queryResponse = null;
        try {
            queryResponse = solrClient.query(collection, solrQuery);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return queryResponse.getResults();
    }

    /**
     * Prints solr docs of a collection
     * @param solrClient
     * @param collection
     */
    public static void printSolrJsonDocs(SolrClient solrClient, String collection) {

        SolrDocumentList solrDocumentList = getSolrDocs(solrClient, collection);

        System.out.println("Solr contains " + solrDocumentList.size() +
                " document(s):\n" + JSONUtil.toJSON(solrDocumentList));
    }

    /**
     * Prints solr chunks docs of a collection, uncompressing measures in a human readable way
     * @param solrClient
     * @param collection
     */
    public static void printUnChunkedSolrJsonDocs(SolrClient solrClient, String collection) {

        SolrDocumentList solrDocumentList = getSolrDocs(solrClient, collection);

        System.out.println("Solr contains " + solrDocumentList.size() +
                " document(s):");

        SimpleDateFormat sdf = Measure.createUtcDateFormatter("yyyy-MM-dd HH:mm:ss.SSS");
        StringBuilder stringBuilder = new StringBuilder("\n{");

        for (SolrDocument solrDocument : solrDocumentList) {
            for (String field : solrDocument.getFieldNames().stream().sorted().collect(Collectors.toList())) {
                stringBuilder.append("\n  \"").append(field).append("\": ");
                Object value = solrDocument.getFieldValue(field);
                if (field.equals(CHUNK_VALUE)) {
                    // Uncompress the chunk and display points in a human readable way
                    byte[] compressedPoints = BinaryEncodingUtils.decode((String)value);
                    long chunkStart = (Long)solrDocument.getFieldValue(CHUNK_START);
                    long chunkEnd = (Long)solrDocument.getFieldValue(CHUNK_END);
                    try {
                        TreeSet<Measure> measures = BinaryCompactionUtil.unCompressPoints(compressedPoints, chunkStart, chunkEnd);
                        for (Measure measure : measures) {
                            double measureValue = measure.getValue();
                            float quality = measure.getQuality();
                            String readableTimestamp = sdf.format(new Date(measure.getTimestamp()));
                            stringBuilder.append("\n    t=").append(readableTimestamp)
                                    .append(" v=").append(measureValue)
                                    .append(" q=").append(quality);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                } else
                {
                    stringBuilder.append(value);
                }
            }
            stringBuilder.append("\n}");
        }

        System.out.println(stringBuilder);
    }

    /**
     * Checks 2 sets of chunks are equal
     * @param expectedChunks
     * @param actualChunks
     */
    public static void compareChunks(Map<String, Chunk> expectedChunks, Map<String, Chunk> actualChunks) {

        assertEquals(expectedChunks.size(), actualChunks.size(), "Not the same number of chunks");

        for (String chunkId : expectedChunks.keySet()) {
            Chunk expectedChunk = expectedChunks.get(chunkId);
            Chunk actualChunk = actualChunks.get(chunkId);
            assertNotNull(actualChunk, "Missing chunk id " + chunkId +
                    " in actual chunks: " + actualChunks.values());
            assertEquals(expectedChunk, actualChunk, "Actual chunk id " + chunkId + " not equal to expected one");
        }
    }
}
