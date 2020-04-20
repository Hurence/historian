package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.http.ingestion.ImportRequestParser;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import static com.hurence.webapiservice.http.grafana.modele.QueryRequestParam.DEFAULT_BUCKET_SIZE;
import static com.hurence.webapiservice.http.grafana.modele.QueryRequestParam.DEFAULT_SAMPLING_ALGORITHM;
import static org.junit.jupiter.api.Assertions.*;

public class ImportJsonRequestParserTest {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryRequestParserTest.class);

    @Test
    public void testParsingCorrectRequest() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        ArrayList<String> errorMessage = responseAndErrorHolder.errorMessages;
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;

        assertEquals(new ArrayList<String>(), errorMessage);
        assertEquals(correctPoints, requestBody);
    }


    @Test
    public void testNullRequestParsing() {
        JsonArray requestBody = null;
        String message = Assertions.assertThrows(NullPointerException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "Null request body");
    }
    @Test
    public void testEmptyRequestParsing() {
        JsonArray requestBody = new JsonArray();
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "Empty request body");
    }
    @Test
    public void testRequestWithMissingNameField() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 88],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"points\": [\n" +
                "      [1477895624886, 65.2],\n" +
                "      [1477916224516, 14]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "Missing a name for at least one metric");
    }

    @Test
    public void testRequestWithNoStringNameField() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": 1234,\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": 1,\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 88],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature444\",\n" +
                "    \"points\": [\n" +
                "      [1477895624886, 65.2],\n" +
                "      [1477916224516, 14]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "A name is not a string for at least one metric");
    }

    @Test
    public void testRequestWithNullNameField() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": null,\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": null,\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 88],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature444\",\n" +
                "    \"points\": [\n" +
                "      [1477895624886, 65.2],\n" +
                "      [1477916224516, 14]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature444\",\n" +
                "    \"points\": [\n" +
                "      [1477895624886, 65.2],\n" +
                "      [1477916224516, 14]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 2 points for metric with name 'null' because this is not a valid name");
        errors.add("Ignored 2 points for metric with name 'null' because this is not a valid name");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
        assertEquals(errors, responseAndErrorHolder.errorMessages);

    }
    @Test
    public void testRequestWithMissingPointsField() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 88],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature444\"\n" +
                "  }\n" +
                "]");
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        });
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "field 'points' is required");
    }
    @Test
    public void testRequestWithWorngPointsFieldType() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": {\n" +
                "      \"time\":\n" +
                "      [1477916224866, 4.0]\n" +
                "    }\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "field 'points' : {\"time\":[1477916224866,4.0]} is not an array");
    }
    @Test
    public void testRequestWithZeroPointSize() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature111' because this point was an empty array");
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature333' because this point was an empty array");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }
    @Test
    public void testRequestWithInvalidPointSize() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 88],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "Points should be of the form [timestamp, value]");
    }
    @Test
    public void testRequestWithNullDate() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [null, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature111' because its timestamp is null");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }
    @Test
    public void testRequestWithInvalidDateType() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [\"1477917224866\", 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature222' because its timestamp is not a long");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }
    @Test
    public void testRequestWithNonJsonArrayPoint() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      5,\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature222' because it was not an array");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }
    @Test
    public void testRequestWithNullPoint() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      null,\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature222' because it was not an array");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }
    @Test
    public void testRequestWithIntegerValue() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        ArrayList<String> errorMessage = responseAndErrorHolder.errorMessages;
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;

        assertEquals(new ArrayList<String>(), errorMessage);
        assertEquals(correctPoints, requestBody);
    }
    @Test
    public void testRequestWithNonDoubleValue() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, \"string\"],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature222' because its value was not a double");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }
    @Test
    public void testRequestWithNullValue() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, null],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        JsonArray expectedArray = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Ignored 1 points for metric with name 'openSpaceSensors.Temperature222' because its value was not a double");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        assertEquals(errors, responseAndErrorHolder.errorMessages);
        assertEquals(responseAndErrorHolder.correctPoints, expectedArray);
    }

    @Test
    public void testRequestWithAllNonBadRequestErrors() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [],\n" +
                "      [1477916224866, null]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      []\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [\"string\", 447.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String message = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        }).getMessage();
        assertEquals(message, "There is no valid points");
    }

}
