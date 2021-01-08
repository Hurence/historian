package com.hurence.webapiservice.http.api.analytics;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface AnalyticsApi {

    String CLUSTERING_ENDPOINT = "/clustering";
    String DASHBOARDING_ENDPOINT = "/dashboarding";

    default Router getRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.get(CLUSTERING_ENDPOINT)
                .produces("application/json")
                .handler(this::clustering);
        router.post(DASHBOARDING_ENDPOINT)
                .produces("application/json")
                .handler(this::dashboarding);
        return router;
    }

    void root(RoutingContext context);

    /**
     * should return metrics name based on input.
     */
    void clustering(RoutingContext context);

    /**
     * should return fields values based on input.
     */
    void dashboarding(RoutingContext context);

}
