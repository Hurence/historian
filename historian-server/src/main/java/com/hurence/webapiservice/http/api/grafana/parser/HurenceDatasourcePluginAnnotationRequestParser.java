package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestType;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.hurence.webapiservice.http.api.grafana.util.RequestParserUtil.*;

public class HurenceDatasourcePluginAnnotationRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestParser.class);

    private final String fromJsonPath;
    private final String toJsonPath;
    private final String tagsJsonPath;
    private final String typeJsonPath;
    private final String limitJsonPath;
    private final String matchAnyJsonPath;

    public HurenceDatasourcePluginAnnotationRequestParser(String fromJsonPath,
                                                          String toJsonPath,
                                                          String tagsJsonPath,
                                                          String typeJsonPath,
                                                          String limitJsonPath,
                                                          String matchAnyJsonPath) {
        this.fromJsonPath = fromJsonPath;
        this.toJsonPath = toJsonPath;
        this.tagsJsonPath = tagsJsonPath;
        this.typeJsonPath = typeJsonPath;
        this.limitJsonPath = limitJsonPath;
        this.matchAnyJsonPath = matchAnyJsonPath;
    }

    public AnnotationRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse annotation requestBody : {}", requestBody);
        AnnotationRequestParam.Builder builder = new AnnotationRequestParam.Builder();
        Long from = parseFrom(requestBody);
        if (from != null) {
            builder.from(from);
        }
        Long to = parseTo(requestBody);
        if (to != null) {
            builder.to(to);
        }
        Integer maxAnnotations = parseMaxAnnotations(requestBody);
        if (maxAnnotations != null) {
            builder.withMaxAnnotation(maxAnnotations);
        }
        List<String> tags = parseTags(requestBody);
        if (tags != null) {
            builder.withTags(tags);
        }
        Boolean matchAny = parseMatchAny(requestBody);;
        if (matchAny != null) {
            builder.withMatchAny(matchAny);
        }
        AnnotationRequestType type = parseType(requestBody);
        if (type != null) {
            builder.withType(type);
        }
        return builder.build();
    }

    private List<String> parseTags(JsonObject requestBody) {
        return parseListString(requestBody, tagsJsonPath);
    }

    private Long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, fromJsonPath);
    }

    private Long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, toJsonPath);
    }


    private AnnotationRequestType parseType(JsonObject requestBody) {
        String typeString = parse(requestBody, typeJsonPath, String.class);
        if (typeString == null) return null;
        try {
            return AnnotationRequestType.valueOf(typeString.toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    String.format("type %s is not supported. supported types are %s",
                            typeString, AnnotationRequestType.getValuesAsString()),
                    ex);
        }
    }

    private Integer parseMaxAnnotations(JsonObject requestBody) {
        return parse(requestBody, limitJsonPath, Integer.class);
    }

    private Boolean parseMatchAny(JsonObject requestBody) {
        return parse(requestBody, matchAnyJsonPath, Boolean.class);
    }

}
