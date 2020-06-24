package com.hurence.webapiservice.http;

import io.vertx.core.json.JsonObject;

import static com.hurence.webapiservice.http.HttpServerVerticle.*;

public class HttpVerticleConf {

    private final String historianAdress;
    private final boolean isDebugModeEnabled;
    private final int maxDataPointsAllowedForExportCsv;
    private final int port;
    private final String hostname;
    private final String uploadDirectory;

    public HttpVerticleConf(JsonObject json) {
        this.isDebugModeEnabled = json.getBoolean(CONFIG_DEBUG_MODE, false);
        this.historianAdress = json.getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        this.maxDataPointsAllowedForExportCsv = json.getInteger(CONFIG_MAX_CSV_POINTS_ALLOWED, 10000);
        this.port = json.getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
        this.hostname = json.getString(CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
        this.uploadDirectory = json.getString(CONFIG_UPLOAD_DIRECTORY, "/tmp/hurence-historian");
    }


    public String getHistorianServiceAdr() {
        return historianAdress;
    }

    public int getMaxDataPointsAllowedForExportCsv() {
        return maxDataPointsAllowedForExportCsv;
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
                ", maxDataPointsAllowedForExportCsv=" + maxDataPointsAllowedForExportCsv +
                ", port=" + port +
                ", hostname='" + hostname + '\'' +
                '}';
    }

    public String getUploadDirectory() {
        return uploadDirectory;
    }
}
