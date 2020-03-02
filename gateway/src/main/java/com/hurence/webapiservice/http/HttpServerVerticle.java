package com.hurence.webapiservice.http;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.grafana.GrafanaApiImpl;
import com.hurence.webapiservice.http.ingestion.IngestionApiImpl;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HistorianService historianService;


    @Override
    public void start(Promise<Void> promise) throws Exception {
        LOGGER.debug("deploying {} verticle with config : {}", HttpServerVerticle.class.getSimpleName(), config().encodePrettily());
        String historianServiceAdr = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        historianService = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), historianServiceAdr);

        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);

//    router.route().handler(CookieHandler.create());
        router.route().handler(BodyHandler.create()
                .setDeleteUploadedFilesOnEnd(true)
        );

//    router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));
        Router graphanaApi = new GrafanaApiImpl(historianService).getGraphanaRouter(vertx);
        router.mountSubRouter("/api/grafana", graphanaApi);

        Router importApi = new IngestionApiImpl().getImportRouter(vertx);
        router.mountSubRouter("/historian-server/ingestion", importApi);
//    router.get("/doc/similarTo/:id").handler(this::getSimilarDoc);

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