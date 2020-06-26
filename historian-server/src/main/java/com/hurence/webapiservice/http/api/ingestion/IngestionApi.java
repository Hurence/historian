package com.hurence.webapiservice.http.api.ingestion;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface IngestionApi {

    String JSON_ENDPOINT = "/json";
    String CSV_ENDPOINT = "/csv";

    default Router getImportRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.post(CSV_ENDPOINT)
                .consumes("multipart/form-data")
                .produces("application/json")
                .handler(this::importCsv);
        router.post(JSON_ENDPOINT)
                .produces("application/json")
                .handler(this::importJson);

        return router;
    }

    /**
     * should post points based on json input.
     * @param context
     */
    void importJson(RoutingContext context);


    void importCsv(RoutingContext context);
}
