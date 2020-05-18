package com.hurence.webapiservice.http.api.test;


import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface TestHistorianApi {

    String QUERY_CHUNK_ENDPOINT = "/chunks";

    default Router getMainRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.post(QUERY_CHUNK_ENDPOINT)
                .produces("application/json")
                .handler(this::getTimeSerieChunks);
        return router;
    }

    /**
     * should return 200 ok
     * @param context
     */
    void root(RoutingContext context);

    /**
     * should return metrics based on input.
     * @param context
     */
    void getTimeSerieChunks(RoutingContext context);
}
