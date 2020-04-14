package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestParam;

import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

import static com.hurence.webapiservice.http.api.grafana.util.DateRequestParserUtil.*;

public class AnnotationRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestParser.class);



    public AnnotationRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse annotation requestBody : {}", requestBody);
        AnnotationRequestParam.Builder builder = new AnnotationRequestParam.Builder();
        Long from = parseFrom(requestBody);
        builder.from(from == null ? AnnotationRequestParam.DEFAULT_FROM : from);
        Long to = parseTo(requestBody);
        builder.to(to == null ? AnnotationRequestParam.DEFAULT_TO : to);
        int maxAnnotations = parseMaxAnnotations(requestBody);
        builder.withMaxAnnotation(maxAnnotations);
        List<String> tags = parseTags(requestBody);
        builder.withTags(tags);
        Boolean matchAny = parseMatchAny(requestBody);;
        builder.withMatchAny(matchAny);
        AnnotationRequestType type = parseType(requestBody);
        builder.withType(type);
        return builder.build();

    }


    private List<String> parseTags(JsonObject requestBody) {
        return requestBody.getJsonArray("tags", new JsonArray()).getList();
    }

    private Long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, "/range/from");
    }

    private Long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, "/range/to");
    }

    private AnnotationRequestType parseType(JsonObject requestBody) {
        String typeString = requestBody.getString("type", AnnotationRequestParam.DEFAULT_TYPE.toString());
        try {
            return AnnotationRequestType.valueOf(typeString.toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    String.format("type %s is not supported. supported types are %s",
                            typeString, AnnotationRequestType.getValuesAsString()),
                    ex);
        }
    }

    private int parseMaxAnnotations(JsonObject requestBody) {
        return requestBody.getInteger("limit", AnnotationRequestParam.DEFAULT_MAX_ANNOTATION_TO_RETURN);
    }

    private Boolean parseMatchAny(JsonObject requestBody) {
        return requestBody.getBoolean("matchAny", true);
    }

}
