package com.hurence.webapiservice.util;

import com.hurence.webapiservice.http.api.modele.ContentType;
import com.hurence.webapiservice.http.api.modele.StatusCodes;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxErrorAnswerDescriptionTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(VertxErrorAnswerDescriptionTest.class);

    @Test
    public void test1() {
        VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder().build();
        Assert.assertEquals(StatusCodes.INTERNAL_SERVER_ERROR, error.getStatusCode());
        Assert.assertEquals(StatusMessages.INTERNAL_SERVER_ERROR, error.getStatusMsg());
        Assert.assertNull(error.getContentType());
    }


    @Test
    public void test2() {
        final Exception ex = new Exception();
        final RoutingContext context = new RoutingContext(null);

        VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                .contentType(ContentType.TEXT_CSV)
                .errorMsg("a test")
                .statusCode(145)
                .statusMsg("a status message")
                .throwable(ex)
                .routingContext(context)
                .build();
        Assert.assertEquals(ContentType.TEXT_CSV, error.getContentType());
        Assert.assertEquals("a test", error.getErrorMsg());
        Assert.assertEquals(145, error.getStatusCode());
        Assert.assertEquals("a status message", error.getStatusMsg());
        Assert.assertEquals(ex, error.getThrowable());
        Assert.assertEquals(context, error.getRoutingContext());
    }

}
