package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.MultipartRequestResponseConf;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.http.api.ingestion.util.TimestampUnit;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import io.vertx.reactivex.ext.web.multipart.MultipartForm;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.http.HttpServerVerticle.*;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.*;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ImportCsvEndPointIT {

    private static WebClient webClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCsvEndPointIT.class);
    public static String DEFAULT_TIMESTAMP_FIELD = "timestamp";
    public static String DEFAULT_NAME_FIELD = "name";
    public static String DEFAULT_VALUE_FIELD = "value";
    public static String DEFAULT_QUALITY_FIELD = "quality";

    public static String MAPPING_TIMESTAMP = "mapping.timestamp";
    public static String MAPPING_NAME = "mapping.name";
    public static String MAPPING_VALUE = "mapping.value";
    public static String MAPPING_QUALITY = "mapping.quality";
    public static String MAPPING_TAGS = "mapping.tags";
    public static String FORMAT_DATE = "format_date";
    public static String TIMEZONE_DATE = "timezone_date";
    public static String GROUP_BY = "group_by";

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {

        SolrITHelper.createChunkCollection(client, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0.toString());
        SolrITHelper.addCodeInstallAndSensor(container);
        SolrITHelper.addFieldToChunkSchema(container, "date");
        webClient = HttpITHelper.buildWebClient(vertx);
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    @AfterEach
    public void deleteSolrChunks(SolrClient client) throws IOException, SolrServerException {
        LOGGER.info("delete all chunks");
        UpdateResponse rsp = client.deleteByQuery(SolrITHelper.COLLECTION_HISTORIAN, "*:*");
        client.commit(SolrITHelper.COLLECTION_HISTORIAN);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMinimalCsvFileImport(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints.csv").getFile();
        testMinimalCsvFileImport(vertx, testContext, pathCsvFile);
    }

    private void testMinimalCsvFileImport(Vertx vertx, VertxTestContext testContext, String pathCsvFile) {
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    /*
        Csv without quality
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMinimalCsvFileImport2(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_without_quality.csv").getFile();
        testMinimalCsvFileImport(vertx, testContext, pathCsvFile);
    }

    /*
    schema of csv changed
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMinimalCsvFileImport3(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_shuffled_columns.csv").getFile();
        testMinimalCsvFileImport(vertx, testContext, pathCsvFile);
    }

    /*
        Import should be idem potent. This can be easily done by setting the id of the chunk as a hash of all parameters.
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testIdemPotence(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportHeaderMappingwithTimestamp(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_custom_mapping.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "metric_timestamp")
                .attribute(MAPPING_NAME, "metric_name")
                .attribute(MAPPING_VALUE, "metric_value")
                .attribute(MAPPING_QUALITY, "metric_quality")
                .attribute(FORMAT_DATE, TimestampUnit.MILLISECONDS_EPOCH.toString())
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportHeaderMappingwithTimestamp2(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_custom_mapping_2.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "metric_timestamp_2")
                .attribute(MAPPING_NAME, "metric_name_2")
                .attribute(MAPPING_VALUE, "metric_value_2")
                .attribute(MAPPING_QUALITY, "metric_quality_2")
                .attribute(FORMAT_DATE, TimestampUnit.MILLISECONDS_EPOCH.toString())
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithTags(Vertx vertx, VertxTestContext testContext) {  // if there is tags shouldn't there be tags [..] in response !
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_with_tags.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "timestamp")
                .attribute(MAPPING_NAME, "metric")
                .attribute(MAPPING_VALUE, "value")
                .attribute(MAPPING_QUALITY, "quality")
                .attribute(MAPPING_TAGS, "sensor")
                .attribute(MAPPING_TAGS, "code_install")
                .attribute(FORMAT_DATE, TimestampUnit.MILLISECONDS_EPOCH.toString())
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_with_tags.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx),
                //this query test content of the chunk
                new RequestResponseConf<>(TEST_CHUNK_QUERY_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/request-metric_1.json",
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/expectedResponse_with_tags.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
//    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportGroupByWithSensorTag(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_with_tags.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "timestamp")
                .attribute(MAPPING_NAME, "metric")
                .attribute(MAPPING_VALUE, "value")
                .attribute(MAPPING_QUALITY, "quality")
                .attribute(MAPPING_TAGS, "sensor")
                .attribute(MAPPING_TAGS, "code_install")
                .attribute(FORMAT_DATE, TimestampUnit.MILLISECONDS_EPOCH.toString())
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .attribute(GROUP_BY, "tags.sensor")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_grouped_by_sensor.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx),
                new RequestResponseConf<>(TEST_CHUNK_QUERY_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/request-metric_1.json",
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/expectedResponse_grouped_by_sensor.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportGroupByWithOtherThanTagsShouldFail(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_with_tags.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "timestamp")
                .attribute(MAPPING_NAME, "metric")
                .attribute(MAPPING_VALUE, "value")
                .attribute(MAPPING_QUALITY, "quality")
                .attribute(MAPPING_TAGS, "sensor")
                .attribute(MAPPING_TAGS, "code_install")
                .attribute(FORMAT_DATE, TimestampUnit.MILLISECONDS_EPOCH.toString())
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .attribute(GROUP_BY, DEFAULT_TIMESTAMP_FIELD)
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_grouped_by_other_than_tags.json",
                        BAD_REQUEST, StatusMessages.BAD_REQUEST,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    /*
    Utc dates "yyyy-MM-dd'T'HH:mm:ss.SSS"
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithStringDates(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_string_date_utc.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, "yyyy-MM-dd'T'HH:mm:ss.SSS")
                .attribute(TIMEZONE_DATE, "UTC")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    /*
        utc dates "yyyy-MM-dd HH:mm:ss.SSS"
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithStringDates2(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_string_date_utc_2.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, "yyyy-D-m HH:mm:ss.SSS")
                .attribute(TIMEZONE_DATE, "UTC")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    /*
        Asia dates
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithStringDates3(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_string_date_asia.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, "yyyy-MM-dd HH:mm:ss.SSS")
                .attribute(TIMEZONE_DATE, "Asia/Aden")//UTC +3
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithStringDatesDefautUTC(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_string_date_utc_2.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, "yyyy-D-m HH:mm:ss.SSS")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
//    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithATagDate(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_with_date_tag.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TAGS, "date")
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .textFileUpload("my_csv_file", "datapoints_with_date_tag.csv", pathCsvFile, "text/csv");

        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_with_date_tag.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<>(TEST_CHUNK_QUERY_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/request-metric_1.json",
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/expectedResponse_with_date_tag.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportSeveralFiles(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints.csv").getFile();
        String pathCsvFile2 = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints2.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile2, "text/csv")
                .textFileUpload("my_csv_file_2", "datapoints2.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse2.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request2.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse2.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportFileLimitNumberOfLineShouldPass(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/csv_5000_lines.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse5000line.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportFileTooBig(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/csv_5001_lines.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "csv_5001_lines.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_too_big.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportSeveralFileSomeTooBig(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFileTooBig = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/csv_5001_lines.csv").getFile();
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("csv_5001_lines", "csv_5001_lines.csv", pathCsvFileTooBig, "text/csv")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_some_files_too_big.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportAChunkByDay(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_on_several_days.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_on_several_days.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse_on_several_days.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx),
                new RequestResponseConf<>(TEST_CHUNK_QUERY_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/request-metric_1.json",
                        "/http/ingestion/csv/onemetric-3points/testQueryChunk/expectedResponse_on_several_days.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonObject(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithSecondTimestampDate(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_second_date.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, TimestampUnit.SECONDS_EPOCH.toString())
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_second_date.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse_second_date.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithNanoSecondTimestampDate(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_nano_second_date.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, TimestampUnit.NANOSECONDS_EPOCH.toString())
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_second_date.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse_second_date.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithMicroSecondTimestampDate(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_micro_second_date.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(FORMAT_DATE, TimestampUnit.MICROSECONDS_EPOCH.toString())
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_second_date.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse_second_date.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportWithFailedPoints(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/onemetric-3points/csvfiles/datapoints_with_failed_points.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "timestamp")
                .attribute(MAPPING_NAME, "metric")
                .attribute(MAPPING_VALUE, "value")
                .attribute(MAPPING_QUALITY, "quality")
                .attribute(MAPPING_TAGS, "sensor")
                .attribute(MAPPING_TAGS, "code_install")
                .attribute(FORMAT_DATE, "yyyy-D-m HH:mm:ss.SSS")
                .attribute(GROUP_BY, DEFAULT_NAME_FIELD)
                .attribute(GROUP_BY, "tags.sensor")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/onemetric-3points/testImport/expectedResponse_with_failed_points.json",
                        CREATED, StatusMessages.CREATED,
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/ingestion/csv/onemetric-3points/testQuery/request3.json",
                        "/http/ingestion/csv/onemetric-3points/testQuery/expectedResponse_with_failed_points.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}

