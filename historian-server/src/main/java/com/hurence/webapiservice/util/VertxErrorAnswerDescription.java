package com.hurence.webapiservice.util;

import com.hurence.webapiservice.http.api.modele.ContentType;
import com.hurence.webapiservice.http.api.modele.StatusCodes;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class describe an error that occured. It also contains :
 * - Vertx RoutingContext connected to the error
 * - Logger where the error was produced.
 *
 * It is used to build an http answer describing the error with the routingContext.
 * This class is just a container class.
 *
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VertxErrorAnswerDescription {

    private static final Logger LOGGER = LoggerFactory.getLogger(VertxErrorAnswerDescription.class);

    @Builder.Default
    private ContentType contentType = ContentType.APPLICATION_JSON;
//    private Logger logger;
    private Throwable throwable;
    @Builder.Default
    private String errorMsg = "";
    private RoutingContext routingContext;
    @Builder.Default
    private String statusMsg = StatusMessages.INTERNAL_SERVER_ERROR;
    @Builder.Default
    private int statusCode = StatusCodes.INTERNAL_SERVER_ERROR;

    public static class VertxErrorAnswerDescriptionBuilder {

    }
}
