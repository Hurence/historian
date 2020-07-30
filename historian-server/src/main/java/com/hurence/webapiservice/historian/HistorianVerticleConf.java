package com.hurence.webapiservice.historian;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianVerticle.*;

public class HistorianVerticleConf {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleConf.class);

    private final String historianAdress;
    private final boolean usingZookeeper;
    private List<String> solrUrls;
    private List<String> solrZookeeperUrls;
    private String zookeeperRoot;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final SolrHistorianConf solrHistorianConf;

    public HistorianVerticleConf(JsonObject conf) {
        this.historianAdress = conf.getString(CONFIG_HISTORIAN_ADDRESS, "historian");
        final JsonObject slrConfig = conf.getJsonObject(CONFIG_ROOT_SOLR);
        this.connectionTimeout = slrConfig.getInteger(CONFIG_SOLR_CONNECTION_TIMEOUT, 10000);
        this.socketTimeout = slrConfig.getInteger(CONFIG_SOLR_SOCKET_TIMEOUT, 60000);
        this.usingZookeeper = slrConfig.getBoolean(CONFIG_SOLR_USE_ZOOKEEPER, false);
        if (!usingZookeeper) {
            this.solrUrls = getStringListIfExist(slrConfig, CONFIG_SOLR_URLS)
                    .orElseThrow(() -> new IllegalArgumentException(String.format(
                            "property %s is needed when not using zookeeper", CONFIG_ROOT_SOLR + "/" + CONFIG_SOLR_URLS
                    )));
        } else {
            this.solrZookeeperUrls = getStringListIfExist(slrConfig, CONFIG_SOLR_ZOOKEEPER_URLS).orElse(Collections.emptyList());//TODO exception ?
            this.zookeeperRoot = slrConfig.getString(CONFIG_SOLR_ZOOKEEPER_ROOT);
        }
        this.solrHistorianConf = parseSolrHistorianConf(conf);
    }

    public String getHistorianServiceAddress() {
        return historianAdress;
    }

    public boolean isUsingZookeeper() {
        return usingZookeeper;
    }

    public List<String> getSolrZookeepersUrls() {
        return solrZookeeperUrls;
    }

    public Optional<String> getZookeeperRoot() {
        return Optional.ofNullable(zookeeperRoot);
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public SolrHistorianConf getHistorianConf() {
        return solrHistorianConf;
    }

    private SolrHistorianConf parseSolrHistorianConf(JsonObject conf) {
        final JsonObject slrConfig = conf.getJsonObject(CONFIG_ROOT_SOLR);
        final String streamEndpoint = slrConfig.getString(CONFIG_SOLR_STREAM_ENDPOINT);
        if (streamEndpoint == null)
            throw new IllegalArgumentException(String.format("key %s is needed in solr config of historian verticle conf.",
                    CONFIG_SOLR_STREAM_ENDPOINT));

        final String chunkCollection = slrConfig.getString(CONFIG_SOLR_CHUNK_COLLECTION, "historian");
        final String annotationCollection = slrConfig.getString(CONFIG_SOLR_ANNOTATION_COLLECTION, "annotation");
        final long limitNumberOfPoint = conf.getLong(CONFIG_LIMIT_NUMBER_OF_POINT, 50000L);
        final long limitNumberOfChunks = conf.getLong(CONFIG_LIMIT_NUMBER_OF_CHUNK, 50000L);
        final int maxNumberOfTargetReturned = getMaxNumberOfTargetReturned(conf);
        SolrHistorianConf historianConf = new SolrHistorianConf();
        historianConf.chunkCollection = chunkCollection;
        historianConf.annotationCollection = annotationCollection;
        historianConf.streamEndPoint = streamEndpoint;
        historianConf.limitNumberOfPoint = limitNumberOfPoint;
        historianConf.limitNumberOfChunks = limitNumberOfChunks;
        historianConf.sleepDurationBetweenTry = slrConfig.getLong(CONFIG_SOLR_SLEEP_BETWEEEN_TRY, 10000L);
        historianConf.numberOfRetryToConnect = slrConfig.getInteger(CONFIG_SOLR_NUMBER_CONNECTION_ATTEMPT, 3);
        historianConf.maxNumberOfTargetReturned = maxNumberOfTargetReturned;
        String schemaVersion = conf.getString(CONFIG_SCHEMA_VERSION, SchemaVersion.VERSION_1.toString());
        historianConf.schemaVersion = SchemaVersion.valueOf(schemaVersion);
        historianConf.client = createSolrClient();
        return historianConf;
    }

    private int getMaxNumberOfTargetReturned(JsonObject conf) {
        if (conf.containsKey(CONFIG_API_HISTORAIN) &&
                conf.getJsonObject(CONFIG_API_HISTORAIN).containsKey(CONFIG_GRAFANA_HISTORAIN) &&
                conf.getJsonObject(CONFIG_API_HISTORAIN).getJsonObject(CONFIG_GRAFANA_HISTORAIN).containsKey(CONFIG_SEARCH_HISTORAIN)
        ) {
            return conf.getJsonObject(CONFIG_API_HISTORAIN)
                    .getJsonObject(CONFIG_GRAFANA_HISTORAIN)
                    .getJsonObject(CONFIG_SEARCH_HISTORAIN)
                    .getInteger(CONFIG_DEFAULT_SIZE_HISTORAIN, 100);
        }
        return 100;
    }

    private CloudSolrClient createSolrClient() {
        CloudSolrClient.Builder clientBuilder;
        if (this.isUsingZookeeper()) {
            LOGGER.info("Zookeeper mode");
            clientBuilder = new CloudSolrClient.Builder(
                    this.getSolrZookeepersUrls(),
                    this.getZookeeperRoot()
            );
        } else {
            LOGGER.info("Client without zookeeper");
            clientBuilder = new CloudSolrClient.Builder(this.solrUrls);
        }
        return clientBuilder
                .withConnectionTimeout(this.getConnectionTimeout())
                .withSocketTimeout(this.getSocketTimeout())
                .build();
    }

    private Optional<List<String>> getStringListIfExist(JsonObject config, String key) {
        JsonArray array = config.getJsonArray(key);
        if (array == null) return Optional.empty();
        return Optional.of(array.stream()
                .map(Object::toString)
                .collect(Collectors.toList()));
    }


    @Override
    public String toString() {
        return "HistorianVerticleConf{" +
                "historianAdress='" + historianAdress + '\'' +
                ", usingZookeeper=" + usingZookeeper +
                ", solrUrls=" + solrUrls +
                ", solrZookeeperUrls=" + solrZookeeperUrls +
                ", zookeeperRoot='" + zookeeperRoot + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", socketTimeout=" + socketTimeout +
                ", solrHistorianConf=" + solrHistorianConf +
                '}';
    }
}
