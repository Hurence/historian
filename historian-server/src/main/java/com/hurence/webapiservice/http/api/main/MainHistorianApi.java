package com.hurence.webapiservice.http.api.main;


import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface MainHistorianApi {

    String EXPORT_ENDPOINT = "/export/csv";
    String SEARCH_ENDPOINT = "/search";
    String QUERY_ENDPOINT = "/query";

    default Router getMainRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.post(SEARCH_ENDPOINT).handler(this::search);
        router.get(QUERY_ENDPOINT)
                .produces("application/json")
                .handler(this::getTimeSeries);
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
     *  used to find metrics available to use query endpoint {@link #getTimeSeries(RoutingContext) query}
     * @param context
     * Expected request exemple :
     * <pre>
     * {
     *   metric: 'upper_50',
     *   limit: 50
     * }
     * </pre>
     * response Exemple :
     * <pre>
     *     ["upper_25","upper_50","upper_75","upper_90","upper_95"]
     * </pre>
     *
     */
    void search(RoutingContext context);

    /**
     * should return metrics based on input.
     * @param context
     */
    void getTimeSeries(RoutingContext context);

    /**
     * should return metrics based on input as csv.
     * @param context
     */
    void export(RoutingContext context);
}
