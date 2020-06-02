package com.hurence.webapiservice.http.api.grafana;


import com.hurence.webapiservice.historian.reactivex.HistorianService;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface GrafanaApi {

    static GrafanaApi fromVersion(GrafanaApiVersion grafanaApiVersion,
                                  HistorianService historianService) {
        switch (grafanaApiVersion) {
            case SIMPLE_JSON_PLUGIN:
                return new GrafanaSimpleJsonPluginApiImpl(historianService);
            case HURENCE_DATASOURCE_PLUGIN:
                return new GrafanaHurenceDatasourcePluginApiImpl(historianService);
            default:
                throw new IllegalArgumentException(String.format("version %s is not yet supported !", grafanaApiVersion));
        }
    }

    String SEARCH_ENDPOINT = "/search";
    String QUERY_ENDPOINT = "/query";
    String ANNOTATIONS_ENDPOINT = "/annotations";
    String TAG_KEYS_ENDPOINT = "/tag-keys";
    String TAG_VALUES_ENDPOINT = "/tag-values";

    default Router getGraphanaRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.post(SEARCH_ENDPOINT).handler(this::search);
        router.post(QUERY_ENDPOINT)
                .produces("application/json")
                .handler(this::query);
        router.post(ANNOTATIONS_ENDPOINT).handler(this::annotations);
        router.post(TAG_KEYS_ENDPOINT).handler(this::tagKeys);
        router.post(TAG_VALUES_ENDPOINT).handler(this::tagValues);
        return router;
    }

    /**
     * should return 200 ok
     * @param context
     */
    void root(RoutingContext context);

    /**
     * should return metrics name based on input.
     */
    void search(RoutingContext context);

    /**
     * should return datapoints of metrics based on input.
     * @param context
     */
    void query(RoutingContext context);

    /**
     * should return annotations based on input.
     * @param context
     */
    void annotations(RoutingContext context);

    /**
     * should return tag keys for ad hoc filters.
     * @param context
     */
    void tagKeys(RoutingContext context);

    /**
     * should return tag values for ad hoc filters.
     * @param context
     */
    void tagValues(RoutingContext context);
}
