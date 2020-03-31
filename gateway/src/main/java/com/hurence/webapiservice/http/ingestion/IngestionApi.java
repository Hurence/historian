package com.hurence.webapiservice.http.ingestion;


import com.hurence.logisland.record.FieldDictionary;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface IngestionApi {

    default Router getImportRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.post("/csv")
                .produces("application/json")
                .handler(this::importCsv);
        router.post("/json").handler(this::importJson);
        return router;
    }

    /**
     * should post points based on json input.
     * @param context
     */
    void importJson(RoutingContext context);

    /**
     *  used by the find metric options on the query tab in panels.
     *  In our case we will return each different '{@value FieldDictionary#RECORD_NAME}' value in historian.
     * @param context
     * Expected request exemple :
     * <pre>
     * { target: 'upper_50' }
     * </pre>
     * response Exemple :
     * <pre>
     *     ["upper_25","upper_50","upper_75","upper_90","upper_95"]
     * </pre>
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     *
     *
     */
    void importCsv(RoutingContext context);
}
