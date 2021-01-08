package com.hurence.webapiservice.util;

import com.hurence.historian.util.ErrorMsgHelper;
import com.hurence.webapiservice.http.api.modele.ContentType;
import com.hurence.webapiservice.http.api.modele.Headers;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.http.HttpServerResponse;

public class VertxHttpErrorMsgHelper {

    public static void answerWithError(VertxErrorAnswerDescription description) {
        HttpServerResponse rsp = description.getRoutingContext().response()
                .setStatusCode(description.getStatusCode())
                .setStatusMessage(description.getStatusMsg());

        switch (description.getContentType()) {
            case TEXT_PLAIN:
                rsp = rsp.putHeader(Headers.contentType, ContentType.TEXT_PLAIN.contentType);
                String errorMsg = ErrorMsgHelper.createMsgError(description.getErrorMsg(), description.getThrowable());
                rsp.end(errorMsg);
                break;
            case APPLICATION_JSON:
                rsp.putHeader(Headers.contentType, ContentType.APPLICATION_JSON.contentType);
                JsonObject errorObject = createJsonError(description.getErrorMsg(), description.getThrowable());
                rsp.end(errorObject.encodePrettily());
                break;
            case TEXT_CSV:
                throw new IllegalArgumentException("TEXT_CSV is not supported");
            default:
                throw new IllegalArgumentException(description.getContentType() + " is not supported");
        }
    }

    public static void answerWithErrorAsText(VertxErrorAnswerDescription description) {
        description.setContentType(ContentType.TEXT_PLAIN);
        answerWithError(description);
    }

    public static void answerWithErrorAsJson(VertxErrorAnswerDescription description) {
        description.setContentType(ContentType.APPLICATION_JSON);
        answerWithError(description);
    }


    public static JsonObject createJsonError(String errorMsg, Throwable t) {
        JsonObject errorObject = new JsonObject();
        if (errorMsg != null) {
            errorObject
                    .put("Error Message", errorMsg);
        }
        if (t != null) {
            errorObject
                    .put("Exception Class", t.getClass().getCanonicalName())
                    .put("Exception Message", t.getMessage());
        }
        return errorObject;
    }
}
