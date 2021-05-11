package com.hurence.webapiservice.http.api.grafana.promql.router;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface GrafanaPromQLDatasourcePluginApi {

    String RULES_ENDPOINT = "/rules";
    String LABELS_ENDPOINT = "/label/__name__/values";
    String QUERY_ENDPOINT = "/query";
    String QUERY_RANGE_ENDPOINT = "/query_range";
    String SERIES_ENDPOINT = "/series";
    String METADATA_ENDPOINT = "/metadata";

    default Router getGraphanaPrometheusRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.get(RULES_ENDPOINT)
                .produces("application/json")
                .handler(this::rules);
        router.get(LABELS_ENDPOINT)
                .produces("application/json")
                .handler(this::labels);
        router.post(QUERY_ENDPOINT)
                .produces("application/json")
                .blockingHandler(this::query);
        router.post(QUERY_RANGE_ENDPOINT)
                .produces("application/json")
                .handler(this::queryRange);
        router.get(SERIES_ENDPOINT)
                .produces("application/json")
                .handler(this::series);
        router.get(METADATA_ENDPOINT)
                .produces("application/json")
                .handler(this::metadata);
        return router;
    }

    void root(RoutingContext context);

    /**
     * should return alerting rules based on input.
     */
    void rules(RoutingContext context);

    /**
     * should return label values based on input.
     */
    void labels(RoutingContext context);

    /**
     * should return the list of series.
     */
    void series(RoutingContext context);

    /**
     * should return datapoints of metrics based on input.
     * @param context
     */
    void query(RoutingContext context);

    /**
     * should return datapoints of metrics based on input.
     * @param context
     */
    void queryRange(RoutingContext context);

    /**
     * should return annotations based on input.
     * @param context
     */
    void metadata(RoutingContext context);
}
