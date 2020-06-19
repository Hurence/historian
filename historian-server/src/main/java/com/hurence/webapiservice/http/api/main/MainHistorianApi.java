package com.hurence.webapiservice.http.api.main;


import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface MainHistorianApi {

    String EXPORT_ENDPOINT = "/export/csv";

    default Router getMainRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.post(EXPORT_ENDPOINT)
                .produces("text/csv")
                .handler(this::export);
        return router;
    }

    /**
     * should return 200 ok
     * @param context
     */
    void root(RoutingContext context);


    /**
     * should return metrics based on input as csv.
     * @param context
     */
    void export(RoutingContext context);
}
