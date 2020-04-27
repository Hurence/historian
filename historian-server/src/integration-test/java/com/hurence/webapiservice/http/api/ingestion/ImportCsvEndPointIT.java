package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.MultipartRequestResponseConf;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
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
import org.junit.jupiter.api.AfterAll;
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

import static com.hurence.webapiservice.http.Codes.OK;
import static com.hurence.webapiservice.http.HttpServerVerticle.IMPORT_CSV_ENDPOINT;
import static com.hurence.webapiservice.http.HttpServerVerticle.QUERY_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ImportCsvEndPointIT {

    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper1;
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCsvEndPointIT.class);
    public static String MAPPING_TIMESTAMP = "mapping.timestamp";
    public static String MAPPING_NAME = "mapping.name";
    public static String MAPPING_VALUE = "mapping.value";
    public static String MAPPING_QUALITY = "mapping.quality";
    public static String MAPPING_TAGS = "mapping.tags";
    public static String FORMAT_DATE = "format_date";
    public static String GROUP_BY = "group_by";

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper1 = new AssertResponseGivenRequestHelper(webClient, IMPORT_CSV_ENDPOINT);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMinimalCsvFileImport(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/testMinimalCsvFileImport/testImport/datapoints.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/testMinimalCsvFileImport/testImportexpectedResponse.json",
                        OK, "OK",
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(QUERY_ENDPOINT,
                        "/http/ingestion/csv/testMinimalCsvFileImport/testQuery/request.json",
                        "/http/ingestion/csv/testMinimalCsvFileImport/testQueryexpectedResponse.json",
                        OK, "OK",
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportHeaderMappingwithTimestamp(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/testCsvFileImportHeaderMappingwithTimestamp/testImport/datapoints.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "metric_timestamp")
                .attribute(MAPPING_NAME, "metric_name")
                .attribute(MAPPING_VALUE, "metric_value")
                .attribute(MAPPING_QUALITY, "metric_quality")
                .attribute(MAPPING_TAGS, "sensor")
                .attribute(MAPPING_TAGS, "code_install")
                .attribute(FORMAT_DATE, "TIMESTAMP_IN_MILLISECONDS")
                .attribute(GROUP_BY, "name")
                .attribute(GROUP_BY, "tags.sensor")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/testMinimalCsvFileImport/testImportexpectedResponse.json",
                        OK, "OK",
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(QUERY_ENDPOINT,
                        "/http/ingestion/csv/testMinimalCsvFileImport/testQuery/request.json",
                        "/http/ingestion/csv/testMinimalCsvFileImport/testQueryexpectedResponse.json",
                        OK, "OK",
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCsvFileImportHeaderMappingwithTimestamp2(Vertx vertx, VertxTestContext testContext) {
        String pathCsvFile = AssertResponseGivenRequestHelper.class.getResource("/http/ingestion/csv/testCsvFileImportHeaderMappingwithTimestamp2/testImport/datapoints.csv").getFile();
        MultipartForm multipartForm = MultipartForm.create()
                .attribute(MAPPING_TIMESTAMP, "metric_timestamp_2")
                .attribute(MAPPING_NAME, "metric_name_2")
                .attribute(MAPPING_VALUE, "metric_value_2")
                .attribute(MAPPING_QUALITY, "metric_quality_2")
                .attribute(MAPPING_TAGS, "sensor")
                .attribute(MAPPING_TAGS, "code_install")
                .attribute(FORMAT_DATE, "TIMESTAMP_IN_MILLISECONDS")
                .attribute(GROUP_BY, "name")
                .attribute(GROUP_BY, "tags.sensor")
                .textFileUpload("my_csv_file", "datapoints.csv", pathCsvFile, "text/csv");
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new MultipartRequestResponseConf<JsonObject>(IMPORT_CSV_ENDPOINT,
                        multipartForm,
                        "/http/ingestion/csv/testMinimalCsvFileImport/testImportexpectedResponse.json",
                        OK, "OK",
                        BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(QUERY_ENDPOINT,
                        "/http/ingestion/csv/testMinimalCsvFileImport/testQuery/request.json",
                        "/http/ingestion/csv/testMinimalCsvFileImport/testQueryexpectedResponse.json",
                        OK, "OK",
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}

