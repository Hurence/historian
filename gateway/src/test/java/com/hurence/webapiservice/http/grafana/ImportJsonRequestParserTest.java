package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.http.ingestion.ImportRequestParser;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                "      [1477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String errorMessage = new ImportRequestParser().checkRequest(requestBody)[0];

        assertNull(errorMessage);
    }


    @Test
    public void testNullRequestParsing() {
        JsonArray requestBody = null;
        assertEquals("parameter array is NULL", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testEmptyRequestParsing() {
        JsonArray requestBody = new JsonArray();
        assertEquals("parameter array is empty", new ImportRequestParser().checkRequest(requestBody)[0]);
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
                "  }\n" +
                "]");
        assertEquals("field \"name\" does not exist !", new ImportRequestParser().checkRequest(requestBody)[0]);
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
                "  }\n" +
                "]");
        assertEquals("metric name field exist, but should be a string", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testRequestWithMissingPointsField() {
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
                "  }\n" +
                "]");
        assertEquals("field \"points\" does not exist !", new ImportRequestParser().checkRequest(requestBody)[0]);
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
        assertEquals("points field exist, but should be of type JsonArray", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testRequestWithInvalidPointsSize() {
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
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        assertEquals("invalid points size [1477895624866]", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testRequestWithNullDate() {
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
        assertEquals("this date value null is not a long !", new ImportRequestParser().checkRequest(requestBody)[0]);
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
        assertEquals("this date value 1477917224866 is not a long !", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testRequestWithInvalidDate() {
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
                "      [2477917224866, 8.0],\n" +
                "      [1477895624866, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        assertEquals("this date  value 2477917224866 could not be parsed as a valid date !", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testRequestWithValidDate() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [1477895624, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        String errorMessage = new ImportRequestParser().checkRequest(requestBody)[0];
        assertNull(errorMessage);
    }
    @Test
    public void testRequestWithInvalidDateLength() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624589, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [147789562463, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        assertEquals("this date  value 147789562463 could not be parsed as a valid date !", new ImportRequestParser().checkRequest(requestBody)[0]);
    }@Test
    public void testRequestWithInvalidDate2() {
        JsonArray requestBody = new JsonArray("[\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature111\",\n" +
                "    \"points\": [\n" +
                "      [1477895624589, 2.0],\n" +
                "      [1477916224866, 4.0]\n" +
                "    ]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"openSpaceSensors.Temperature222\",\n" +
                "    \"points\": [\n" +
                "      [1477917224866, 8.0],\n" +
                "      [9477895624, 43.0]\n" +
                "    ]\n" +
                "  }\n" +
                "]");
        assertEquals("this date  value 9477895624 could not be parsed as a valid date !", new ImportRequestParser().checkRequest(requestBody)[0]);
    }

    @Test
    public void testRequestWithInvalidValue() {
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
        assertEquals("invalid Value : not a double 8", new ImportRequestParser().checkRequest(requestBody)[0]);
    }
    @Test
    public void testRequestWithNullValue() {
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
        assertEquals("invalid Value : not a double null", new ImportRequestParser().checkRequest(requestBody)[0]);
    }

}
