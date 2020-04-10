package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.http.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.grafana.modele.AnnotationRequestType;
import com.hurence.webapiservice.http.grafana.parser.AnnotationRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AnnotationRequestParserTest {

    private static Logger LOGGER = LoggerFactory.getLogger(AnnotationRequestParserTest.class);
    @Test
    public void testParsingRequest() {
        JsonObject requestBody = new JsonObject()
                .put("range", new JsonObject().put("from", "2020-2-14T04:43:14.070Z").put("to", "2020-2-14T07:43:14.070Z"))
                .put("limit", 2)
                .put("matchAny", true)
                .put("type", "ALL");
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1581655394070L, request.getFrom());
        assertEquals(1581666194070L, request.getTo());
        assertEquals(true, request.getMatchAny());
        assertEquals(2, request.getMaxAnnotation());
        assertEquals(AnnotationRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(AnnotationRequestType.ALL, request.getType());
    }

    @Test
    public void testParsingTypeLowerCase() {
        JsonObject requestBody = new JsonObject()
                .put("range", new JsonObject().put("from", "2020-2-14T04:43:14.070Z").put("to", "2020-2-14T07:43:14.070Z"))
                .put("limit", 2)
                .put("matchAny", true)
                .put("type", "all");
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1581655394070L, request.getFrom());
        assertEquals(1581666194070L, request.getTo());
        assertEquals(true, request.getMatchAny());
        assertEquals(2, request.getMaxAnnotation());
        assertEquals(AnnotationRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(AnnotationRequestType.ALL, request.getType());
    }

    @Test
    public void testParsingTypeMixingCase() {
        JsonObject requestBody = new JsonObject()
                .put("range", new JsonObject().put("from", "2020-2-14T04:43:14.070Z").put("to", "2020-2-14T07:43:14.070Z"))
                .put("limit", 2)
                .put("matchAny", true)
                .put("type", "aLl");
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1581655394070L, request.getFrom());
        assertEquals(1581666194070L, request.getTo());
        assertEquals(true, request.getMatchAny());
        assertEquals(2, request.getMaxAnnotation());
        assertEquals(AnnotationRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(AnnotationRequestType.ALL, request.getType());
    }

    @Test
    public void testParsingRequest2() {
        JsonObject requestBody = new JsonObject()
                .put("range", new JsonObject().put("from", "2020-2-14T04:43:14.070Z").put("to", "2020-2-14T07:43:14.070Z"))
                .put("tags", new JsonArray().add("tag1").add("tag2"))
                .put("matchAny", false)
                .put("type", "tags");
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1581655394070L, request.getFrom());
        assertEquals(1581666194070L, request.getTo());
        assertEquals(false, request.getMatchAny());
        assertEquals(AnnotationRequestParam.DEFAULT_MAX_ANNOTATION_TO_RETURN, request.getMaxAnnotation());
        assertEquals(Arrays.asList("tag1","tag2"), request.getTags());
        assertEquals(AnnotationRequestType.TAGS, request.getType());
    }


    @Test
    public void testParsingRequest3() {
        JsonObject requestBody = new JsonObject()
                .put("range", new JsonObject().put("from", "2020-2-14T04:43:14.070Z").put("to", "2020-2-14T07:43:14.070Z"))
                .put("type", "tags");
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1581655394070L, request.getFrom());
        assertEquals(1581666194070L, request.getTo());
        assertEquals(AnnotationRequestParam.DEFAULT_MATCH_ANY, request.getMatchAny());
        assertEquals(AnnotationRequestParam.DEFAULT_MAX_ANNOTATION_TO_RETURN, request.getMaxAnnotation());
        assertEquals(AnnotationRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(AnnotationRequestType.TAGS, request.getType());
    }

    @Test
    public void testParsingRequest4() {
        JsonObject requestBody = new JsonObject()
                .put("range", new JsonObject().put("from", "2020-2-14T04:43:14.070Z").put("to", "2020-2-14T07:43:14.070Z"));
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(1581655394070L, request.getFrom());
        assertEquals(1581666194070L, request.getTo());
        assertEquals(AnnotationRequestParam.DEFAULT_MATCH_ANY, request.getMatchAny());
        assertEquals(AnnotationRequestParam.DEFAULT_MAX_ANNOTATION_TO_RETURN, request.getMaxAnnotation());
        assertEquals(AnnotationRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(AnnotationRequestParam.DEFAULT_TYPE, request.getType());
    }

    @Test
    public void testParsingRequest5() {
        JsonObject requestBody = new JsonObject();
        final AnnotationRequestParser annotationRequestParser = new AnnotationRequestParser();
        final AnnotationRequestParam request = annotationRequestParser.parseRequest(requestBody);
        LOGGER.info("request : {}", request);
        assertEquals(AnnotationRequestParam.DEFAULT_FROM, request.getFrom());
        assertEquals(AnnotationRequestParam.DEFAULT_TO, request.getTo());
        assertEquals(AnnotationRequestParam.DEFAULT_MATCH_ANY, request.getMatchAny());
        assertEquals(AnnotationRequestParam.DEFAULT_MAX_ANNOTATION_TO_RETURN, request.getMaxAnnotation());
        assertEquals(AnnotationRequestParam.DEFAULT_TAGS, request.getTags());
        assertEquals(AnnotationRequestParam.DEFAULT_TYPE, request.getType());
    }
}
