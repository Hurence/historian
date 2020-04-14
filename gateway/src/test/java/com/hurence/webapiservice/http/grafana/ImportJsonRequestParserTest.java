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
    public void testParsingCorrectRequest() throws Exception {
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
                "      [1477917224866, 8.0],\n" +
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
        Assertions.assertThrows(NullPointerException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        });
    }
    @Test
    public void testEmptyRequestParsing() {
        JsonArray requestBody = new JsonArray();
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        });
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
        JsonArray expectedArray = new JsonArray("[\n" +
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
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("can't add this object {\"points\":[[1477895624866,2.0],[1477916224866,4.0]]}\ncan't add this object {\"points\":[[1477895624886,65.2],[1477916224516,14]]}");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;
        StringBuilder errorMessageBuilder = new StringBuilder();
        for (String SingleErrorMessage : responseAndErrorHolder.errorMessages) {
            errorMessageBuilder.append(SingleErrorMessage).append("\n");
        }
        StringBuilder errorMessageBuilderExpected = new StringBuilder();
        for (String SingleErrorMessage : errors) {
            errorMessageBuilderExpected.append(SingleErrorMessage).append("\n");
        }
        assertEquals(errorMessageBuilderExpected.toString(), errorMessageBuilder.toString());
        assertEquals(correctPoints, expectedArray);
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
        Assertions.assertThrows(ClassCastException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        });
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
        JsonArray expectedArray = new JsonArray("[\n" +
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
                "  }\n" +
                "]");
        ArrayList<String> errors = new ArrayList<>();
        errors.add("can't add this object {\"name\":\"openSpaceSensors.Temperature111\"}\ncan't add this object {\"name\":\"openSpaceSensors.Temperature444\"}");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;
        StringBuilder errorMessageBuilder = new StringBuilder();
        for (String SingleErrorMessage : responseAndErrorHolder.errorMessages) {
            errorMessageBuilder.append(SingleErrorMessage).append("\n");
        }
        StringBuilder errorMessageBuilderExpected = new StringBuilder();
        for (String SingleErrorMessage : errors) {
            errorMessageBuilderExpected.append(SingleErrorMessage).append("\n");
        }
        assertEquals(errorMessageBuilderExpected.toString(), errorMessageBuilder.toString());
        assertEquals(correctPoints, expectedArray);
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
        Assertions.assertThrows(ClassCastException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        });
    }
    @Test
    public void testRequestWithInvalidPointsSizeAndMissingNameField() throws Exception {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624866],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [43.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature333\",\n" +
                "    \"points\": [\n" +
                "      [1477895624865],\n" +
                "      [1477916224066, 447.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"points\": [\n" +
                "      [1477895624886, 65.2],\n" +
                "      [1477916224516]\n" +
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
                "      [1477917224866, 8.0]\n" +
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
        errors.add("can't add this point [1477895624866] of this object {\"name\":\"openSpaceSensors.Temperature111\",\"points\":[[1477895624866],[1477916224866,4.0]]}\ncan't add this point [43.0] of this object {\"name\":\"openSpaceSensors.Temperature222\",\"points\":[[1477917224866,8.0],[43.0]]}\n"+
                "can't add this point [1477895624865] of this object {\"name\":\"openSpaceSensors.Temperature333\",\"points\":[[1477895624865],[1477916224066,447.0]]}\ncan't add this object {\"points\":[[1477895624886,65.2],[1477916224516]]}");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;
        StringBuilder errorMessageBuilder = new StringBuilder();
        for (String SingleErrorMessage : responseAndErrorHolder.errorMessages) {
            errorMessageBuilder.append(SingleErrorMessage).append("\n");
        }
        StringBuilder errorMessageBuilderExpected = new StringBuilder();
        for (String SingleErrorMessage : errors) {
            errorMessageBuilderExpected.append(SingleErrorMessage).append("\n");
        }
        assertEquals(errorMessageBuilderExpected.toString(), errorMessageBuilder.toString());
        assertEquals(correctPoints, expectedArray);
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
        errors.add("can't add this point [null,2.0] of this object {\"name\":\"openSpaceSensors.Temperature111\",\"points\":[[null,2.0],[1477916224866,4.0]]}");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        ArrayList<String> errorMessage = responseAndErrorHolder.errorMessages;
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;

        assertEquals(errors, errorMessage);
        assertEquals(correctPoints, expectedArray);
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
        Assertions.assertThrows(ClassCastException.class, () -> {
            ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        });
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
        errors.add("can't add this point [1477917224866,null] of this object {\"name\":\"openSpaceSensors.Temperature222\",\"points\":[[1477917224866,null],[1477895624866,43.0]]}");
        ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(requestBody);
        ArrayList<String> errorMessage = responseAndErrorHolder.errorMessages;
        JsonArray correctPoints = responseAndErrorHolder.correctPoints;

        assertEquals(errors, errorMessage);
        assertEquals(correctPoints, expectedArray);
    }


}
