package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.http.grafana.modele.AnnotationRequestParam;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.hurence.webapiservice.http.grafana.util.DateRequestParserUtil.*;

public class AnnotationRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestParser.class);



    public AnnotationRequestParam parseAnnotationRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse annotation requestBody : {}", requestBody);
        AnnotationRequestParam.Builder builder = new AnnotationRequestParam.Builder();
        long from = parseFrom(requestBody);
        builder.from(from);
        long to = parseTo(requestBody);
        builder.to(to);
        int maxAnnotations = parseMaxAnnotations(requestBody);;
        builder.withMaxAnnotation(maxAnnotations);
        JsonArray tags = parseTags(requestBody);;
        builder.withTags(tags);
        Boolean matchAny = parseMatchAny(requestBody);;
        builder.withMatchAny(matchAny);
        String type = parseType(requestBody);
        builder.withType(type);
        return builder.build();

    }


    private JsonArray parseTags(JsonObject requestBody) {
        return requestBody.getJsonArray("tags");
    }

    private long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, "/range/from");
    }

    private long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, "/range/to");
    }

    private String parseType(JsonObject requestBody) {
        return requestBody.getString("type");
    }    // i should put the defaults here

    private int parseMaxAnnotations(JsonObject requestBody) {
        return requestBody.getInteger("limit");
    }

    private Boolean parseMatchAny(JsonObject requestBody) {
        return requestBody.getBoolean("matchAny", false);
    }

}
