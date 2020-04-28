package com.hurence.webapiservice.http.api.grafana;


import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.AnnotationRequestParser;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.webapiservice.http.StatusCodes.BAD_REQUEST;

public class GrafanaHurenceDatasourcePliginApiImpl extends GrafanaSimpleJsonPluginApiImpl implements GrafanaApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaHurenceDatasourcePliginApiImpl.class);

    public GrafanaHurenceDatasourcePliginApiImpl(HistorianService service) {
        super(service);
    }


    /**
     *  used to the find annotations.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     * {
     *   "range": {
     *     "from": "2020-2-14T01:43:14.070Z",
     *     "to": "2020-2-14T06:50:14.070Z"
     *   },
     *   "rangeRaw": {
     *     "from": "now-1h",
     *     "to": "now"
     *   },
     *   "limit" : 100,
     *   "tags": ["tag1", "tag2"],
     *   "matchAny": false,
     *   "type": "tags"
     * }
     * </pre>
     * response Exemple :
     * <pre>
     * {
     *   "annotations" : [
     *     {
     *       "time": 1581648194070,
     *       "text": "annotation 1",
     *       "tags": ["tag1","tag2"]
     *     }
     *   ],
     *   "total_hit" : 1
     * }
     * </pre>
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     */
    @Override
    public void annotations(RoutingContext context) {
        final AnnotationRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            request = new AnnotationRequestParser().parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getAnnotationParams = buildHistorianAnnotationRequest(request);

        service
                .rxGetAnnotations(getAnnotationParams)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(annotations -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(annotations.encode());
                }).subscribe();
    }
}
