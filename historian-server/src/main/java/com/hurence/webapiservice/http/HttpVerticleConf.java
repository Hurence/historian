package com.hurence.webapiservice.http;

import io.vertx.core.json.JsonObject;

import static com.hurence.webapiservice.http.HttpServerVerticle.*;

public class HttpVerticleConf {

    private final String historianAdress;
    private final boolean isDebugModeEnabled;
    private final int maxDataPointsAllowed;
    private final int port;
    private final String hostname;
    private final String uploadDirectory;

    public static final int CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED_DEFAULT = 50000;

    public HttpVerticleConf(JsonObject json) {
        this.isDebugModeEnabled = json.getBoolean(CONFIG_DEBUG_MODE, false);
        this.historianAdress = json.getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        this.maxDataPointsAllowed = json.getInteger(CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED, CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED_DEFAULT);
        this.port = json.getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
        this.hostname = json.getString(CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
        this.uploadDirectory = json.getString(CONFIG_UPLOAD_DIRECTORY, "/tmp/hurence-historian");
    }


    public String getHistorianServiceAdr() {
        return historianAdress;
    }

    public int getMaxDataPointsAllowed() {
        return maxDataPointsAllowed;
    }

    public boolean isDebugModeEnabled() {
        return isDebugModeEnabled;
    }

    public int getHttpPort() {
        return port;
    }

    public String getHostName() {
        return hostname;
    }

    @Override
    public String toString() {
        return "HttpVerticleConf{" +
                "historianAdress='" + historianAdress + '\'' +
                ", isDebugModeEnabled=" + isDebugModeEnabled +
                ", maxDataPointsAllowedForExportCsv=" + maxDataPointsAllowed +
                ", port=" + port +
                ", hostname='" + hostname + '\'' +
                '}';
    }

    public String getUploadDirectory() {
        return uploadDirectory;
    }
}
