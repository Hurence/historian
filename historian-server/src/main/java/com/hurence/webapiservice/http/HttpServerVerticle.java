package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.grafana.GrafanaApi;
import com.hurence.webapiservice.http.api.grafana.GrafanaApiVersion;
import com.hurence.webapiservice.http.api.main.MainHistorianApiImpl;
import io.vertx.core.Promise;
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
    public static final String GRAFANA = "grafana";
    public static final String VERSION = "version";
//    public static final String API = "api";
//    public static final String SEARCH = "search";
//    public static final String LIMIT_DEFAULT = "limit_default";
    public static final String CONFIG_HISTORIAN_ADDRESS = "historian.address";
    public static final String CONFIG_TIMESERIES_ADDRESS = "timeseries.address";
    public static final String CONFIG_MAX_CSV_POINTS_ALLOWED = "max_data_points_allowed_for_ExportCsv";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;

    public static final String MAIN_API_ENDPOINT = "/api/historian/v0";
    public static final String GRAFANA_API_ENDPOINT = "/api/grafana";
    public static final GrafanaApiVersion GRAFANA_API_VEFSION_DEFAULT = GrafanaApiVersion.SIMPLE_JSON_PLUGIN;

    @Override
    public void start(Promise<Void> promise) throws Exception {
        LOGGER.debug("deploying {} verticle with config : {}", HttpServerVerticle.class.getSimpleName(), config().encodePrettily());
        String historianServiceAdr = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        int maxDataPointsAllowedForExportCsv = config().getInteger(CONFIG_MAX_CSV_POINTS_ALLOWED, 10000);
        historianService = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), historianServiceAdr);

        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());

        Router mainApi = new MainHistorianApiImpl(historianService, maxDataPointsAllowedForExportCsv).getMainRouter(vertx);
        router.mountSubRouter(MAIN_API_ENDPOINT, mainApi);

        GrafanaApiVersion grafanaApiVersion = getGrafanaApiVersion();
        Router graphanaApi = GrafanaApi.fromVersion(grafanaApiVersion, historianService).getGraphanaRouter(vertx);
        router.mountSubRouter(GRAFANA_API_ENDPOINT, graphanaApi);

        int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
        String host = config().getString(CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
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

    private GrafanaApiVersion getGrafanaApiVersion() {
        String apiversion;
        try {
            apiversion = config().getJsonObject(GRAFANA).getString(VERSION, GRAFANA_API_VEFSION_DEFAULT.toString());
        } catch (Exception ex) {
            LOGGER.info("grafana version is not defined. We will use default version which is " +  GRAFANA_API_VEFSION_DEFAULT);
            return GRAFANA_API_VEFSION_DEFAULT;
        }
        return GrafanaApiVersion.valueOf(apiversion.toUpperCase());
    }
}