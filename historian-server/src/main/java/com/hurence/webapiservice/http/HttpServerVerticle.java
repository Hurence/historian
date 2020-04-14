package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.grafana.GrafanaApi;
import com.hurence.webapiservice.http.api.grafana.GrafanaHurenceDatasourcePliginApiImpl;
import com.hurence.webapiservice.http.api.grafana.GrafanaSimpleJsonPluginApiImpl;
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
    public static final String CONFIG_HISTORIAN_ADDRESS = "historian.address";
    public static final String CONFIG_TIMESERIES_ADDRESS = "timeseries.address";
    public static final String CONFIG_MAX_CSV_POINTS_ALLOWED = "max_data_points_allowed_for_ExportCsv";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;


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
        router.mountSubRouter("/api/historian/v0", mainApi);

        GrafanaApiVersion grafanaApiVersion = GrafanaApiVersion.SIMPLE_JSON_PLUGIN;
        Router graphanaApi = GrafanaApi.fromVersion(grafanaApiVersion, historianService).getGraphanaRouter(vertx);
        router.mountSubRouter("/api/grafana", graphanaApi);

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
}