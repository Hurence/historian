package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.analytics.AnalyticsApi;
import com.hurence.webapiservice.http.api.analytics.AnalyticsApiImpl;
import com.hurence.webapiservice.http.api.grafana.*;
import com.hurence.webapiservice.http.api.ingestion.IngestionApi;
import com.hurence.webapiservice.http.api.ingestion.IngestionApiImpl;
import com.hurence.webapiservice.http.api.main.MainHistorianApi;
import com.hurence.webapiservice.http.api.main.MainHistorianApiImpl;
import com.hurence.webapiservice.http.api.test.TestHistorianApi;
import com.hurence.webapiservice.http.api.test.TestHistorianApiImpl;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerVerticle extends AbstractVerticle {

    /*
      CONFS
     */
    public static final String CONFIG_HTTP_SERVER_PORT = "port";
    public static final String CONFIG_HTTP_SERVER_HOSTNAME = "host";
    public static final String CONFIG_UPLOAD_DIRECTORY = "upload_directory";
    public static final String GRAFANA = "grafana";
    public static final String VERSION = "version";
    //    public static final String API = "api";
//    public static final String SEARCH = "search";
//    public static final String LIMIT_DEFAULT = "limit_default";
    public static final String CONFIG_HISTORIAN_ADDRESS = "historian.address";
    public static final String CONFIG_DEBUG_MODE = "debug";
    public static final String CONFIG_TIMESERIES_ADDRESS = "timeseries.address";
    public static final String CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED = "max_data_points_maximum_allowed";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;

    //main
    private static final String MAIN_API_ENDPOINT = "/api/historian/v0";
    public static final String CSV_EXPORT_ENDPOINT = MAIN_API_ENDPOINT + MainHistorianApi.EXPORT_ENDPOINT;
    private static final String IMPORT_ENDPOINT = MAIN_API_ENDPOINT + "/import";
    public static final String IMPORT_CSV_ENDPOINT = IMPORT_ENDPOINT + IngestionApi.CSV_ENDPOINT;
    public static final String IMPORT_JSON_ENDPOINT = IMPORT_ENDPOINT + IngestionApi.JSON_ENDPOINT;

    private static final String ANALYTICS_ENDPOINT = MAIN_API_ENDPOINT + "/analytics";
   // public static final String ANALYTICS_CLUSTERING_ENDPOINT = ANALYTICS_ENDPOINT + AnalyticsApi.CLUSTERING_ENDPOINT;
    //grafana
    private static final String GRAFANA_API_ENDPOINT = "/api/grafana";
    private static final String SIMPLE_JSON_GRAFANA_API_ENDPOINT = GRAFANA_API_ENDPOINT + "/simplejson";
    public static final String SIMPLE_JSON_GRAFANA_QUERY_API_ENDPOINT = HttpServerVerticle.SIMPLE_JSON_GRAFANA_API_ENDPOINT +
            GrafanaSimpleJsonPluginApi.QUERY_ENDPOINT;
    public static final String SIMPLE_JSON_GRAFANA_SEARCH_API_ENDPOINT = HttpServerVerticle.SIMPLE_JSON_GRAFANA_API_ENDPOINT +
            GrafanaSimpleJsonPluginApi.SEARCH_ENDPOINT;
    public static final String SIMPLE_JSON_GRAFANA_ANNOTATIONS_API_ENDPOINT = HttpServerVerticle.SIMPLE_JSON_GRAFANA_API_ENDPOINT +
            GrafanaSimpleJsonPluginApi.ANNOTATIONS_ENDPOINT;
    public static final String SIMPLE_JSON_GRAFANA_TAG_KEYS_API_ENDPOINT = HttpServerVerticle.SIMPLE_JSON_GRAFANA_API_ENDPOINT +
            GrafanaSimpleJsonPluginApi.TAG_KEYS_ENDPOINT;
    public static final String SIMPLE_JSON_GRAFANA_TAG_VALUES_API_ENDPOINT = HttpServerVerticle.SIMPLE_JSON_GRAFANA_API_ENDPOINT +
            GrafanaSimpleJsonPluginApi.TAG_VALUES_ENDPOINT;
    private static final String HURENCE_DATASOURCE_GRAFANA_API_ENDPOINT = GRAFANA_API_ENDPOINT + "/v0";
    public static final String HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT = HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_API_ENDPOINT +
            GrafanaHurenceDatasourcePluginApi.QUERY_ENDPOINT;
    public static final String HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT = HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_API_ENDPOINT +
            GrafanaHurenceDatasourcePluginApi.SEARCH_VALUES_ENDPOINT;
    public static final String HURENCE_DATASOURCE_GRAFANA_SEARCH_TAGS_API_ENDPOINT = HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_API_ENDPOINT +
            GrafanaHurenceDatasourcePluginApi.SEARCH_TAGS_ENDPOINT;
    public static final String HURENCE_DATASOURCE_GRAFANA_ANNOTATIONS_API_ENDPOINT = HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_API_ENDPOINT +
            GrafanaHurenceDatasourcePluginApi.ANNOTATIONS_ENDPOINT;

    //test endpoints
    private static final String TEST_API_ENDPOINT = "/test/api";
    public static final String TEST_CHUNK_QUERY_ENDPOINT = HttpServerVerticle.TEST_API_ENDPOINT + TestHistorianApi.QUERY_CHUNK_ENDPOINT;


    private HttpVerticleConf conf;

    @Override
    public void start(Promise<Void> promise) throws Exception {
        this.conf = parseConfig(config());
        LOGGER.info("deploying {} verticle with config : {}", HistorianVerticle.class.getSimpleName(), this.conf);

        historianService = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), conf.getHistorianServiceAdr());
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create()
                .setUploadsDirectory(conf.getUploadDirectory())
                .setDeleteUploadedFilesOnEnd(true)
        );

        //main
        Router mainApi = new MainHistorianApiImpl(historianService, conf.getMaxDataPointsAllowed())
                .getMainRouter(vertx);
        router.mountSubRouter(MAIN_API_ENDPOINT, mainApi);
        Router importApi = new IngestionApiImpl(historianService).getImportRouter(vertx);
        router.mountSubRouter(IMPORT_ENDPOINT, importApi);

        // analytics
        Router analyticsApi = new AnalyticsApiImpl(historianService).getRouter(vertx);
        router.mountSubRouter(ANALYTICS_ENDPOINT, analyticsApi);

        //grafana
        Router hurenceGraphanaApi = new GrafanaHurenceDatasourcePluginApiImpl(historianService, conf.getMaxDataPointsAllowed())
                .getGraphanaRouter(vertx);
        Router simpleJsonGraphanaApi = new GrafanaSimpleJsonPluginApiImpl(historianService, conf.getMaxDataPointsAllowed())
                .getGraphanaRouter(vertx);
        router.mountSubRouter(SIMPLE_JSON_GRAFANA_API_ENDPOINT, simpleJsonGraphanaApi);
        router.mountSubRouter(HURENCE_DATASOURCE_GRAFANA_API_ENDPOINT, hurenceGraphanaApi);

        //test
        if (conf.isDebugModeEnabled()) {
            Router debugApi = new TestHistorianApiImpl(historianService).getMainRouter(vertx);
            router.mountSubRouter(TEST_API_ENDPOINT, debugApi);
        }

        int portNumber = conf.getHttpPort();
        String host = conf.getHostName();
        server
                .requestHandler(router)
                .listen(portNumber, host, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("HTTP server running at {}:{} (verticle {})", host, portNumber, HttpServerVerticle.class.getSimpleName());
                        promise.complete();
                    } else {
                        String errorMsg = String.format("Could not start a HTTP server at %s:%s (verticle %s)",
                                host, portNumber, HttpServerVerticle.class.getSimpleName());
                        LOGGER.error(errorMsg, ar.cause());
                        promise.fail(ar.cause());
                    }
                });
    }

    public HttpVerticleConf parseConfig(JsonObject config) {
        return new HttpVerticleConf(config);
    }
}