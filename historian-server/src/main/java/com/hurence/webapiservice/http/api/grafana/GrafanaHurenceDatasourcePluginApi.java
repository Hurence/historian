package com.hurence.webapiservice.http.api.grafana;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface GrafanaHurenceDatasourcePluginApi {

    String SEARCH_ENDPOINT = "/search";
    String SEARCH_VALUES = "/values";
    String SEARCH_TAGS = "/tags";
    String SEARCH_VALUES_ENDPOINT = SEARCH_ENDPOINT+SEARCH_VALUES;
    String SEARCH_TAGS_ENDPOINT = SEARCH_ENDPOINT+SEARCH_TAGS;
    String QUERY_ENDPOINT = "/query";
    String ANNOTATIONS_ENDPOINT = "/annotations";

    default Router getGraphanaRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.post(SEARCH_ENDPOINT).handler(this::search);
        router.post(SEARCH_VALUES_ENDPOINT).handler(this::searchValues);
        router.post(SEARCH_TAGS_ENDPOINT).handler(this::searchTags);
        router.post(QUERY_ENDPOINT)
                .produces("application/json")
                .handler(this::query);
        router.post(ANNOTATIONS_ENDPOINT).handler(this::annotations);
        return router;
    }

    /**
     * should return metrics name based on input.
     */
    void search(RoutingContext context);

    /**
     * should return fields values based on input.
     */
    void searchValues(RoutingContext context);

    /**
     * should return the list of tags.
     */
    void searchTags(RoutingContext context);

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
}
