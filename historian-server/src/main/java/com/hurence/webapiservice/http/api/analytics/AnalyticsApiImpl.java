package com.hurence.webapiservice.http.api.analytics;

import com.hurence.historian.util.ErrorMsgHelper;
import com.hurence.timeseries.analysis.clustering.ChunkClusterable;
import com.hurence.timeseries.analysis.clustering.ChunksClustering;
import com.hurence.timeseries.analysis.clustering.KMeansChunksClustering;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.timeseries.model.ChunkWrapper;
import com.hurence.webapiservice.http.api.analytics.model.ClusteringRequest;
import com.hurence.webapiservice.http.api.ingestion.IngestionApiImpl;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import io.vertx.reactivex.ext.web.RoutingContext;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.annotations.Experimental;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hurence.historian.model.FieldNamesInsideHistorianService.CHUNK_SAX;
import static com.hurence.timeseries.model.Definitions.*;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.*;


/**
 * {
 * "names": ["messages"],
 * "day": "2019-11-28",
 * "k": 5
 * }
 */
public class AnalyticsApiImpl implements AnalyticsApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiImpl.class);
    private HistorianService service;


    public AnalyticsApiImpl(HistorianService service) {
        this.service = service;
    }


    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian analytics api is Working fine, try out /clustering");
    }

    /**
     * Build a cluster list of chunks based on sax strings
     *
     * curl --location --request GET 'localhost:8080/api/historian/v0/analytics/clustering' \
     * --header 'Content-Type: application/json' \
     * --data-raw '{
     *     "names": ["ack"],
     *     "day": "2019-11-28",
     *     "k": 5
     * }'
     *
     * return
     *
     * {
     *     "clustering.algo": "kmeans",
     *     "k": 5,
     *     "day": "2019-11-28",
     *     "maxIterations": 100,
     *     "elapsed_time": 358,
     *     "clusters": [
     *         {
     *             "sax_cluster": "0",
     *             "cluster_size": 1,
     *             "chunks": [
     *                 {
     *                     "id": "5ba8f698c82ed652b034d22f04db844ea43531536d19150ccc0071e482791bf4",
     *                     "chunk_sax": "dddddgdddddddddddddd",
     *                     "metric_key": "ack|crit$null|metric_id$621ba06c-d980-4b9f-9018-8688ff17f252|warn$null",
     *                     "chunk_avg": "7.352941176470604E-4"
     *                 }
     *             ]
     *         },
     *         ...
     *     ]
     * }
     *
     * @param context the current vert.x RoutingContext
     */
    @Override
    @Experimental
    public void clustering(RoutingContext context) {

        final long start = System.currentTimeMillis();

        // parse request
        final ClusteringRequest request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            request = ClusteringRequest.fromJson(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(ErrorMsgHelper.createMsgError("Error parsing request !", ex));
            return;
        }

        // get the chunks corresponding to request params
        service.rxGetTimeSeriesChunk(request.toParams())
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "text/plain");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(response -> {

                    // convert the json response into ChunkClusterable objects
                    List<ChunkClusterable> chunkWrappers = response.getJsonArray("chunks").stream()
                            .map(metric -> {
                                JsonObject el = (JsonObject) metric;

                                Map<String, String> tags = new HashMap<String, String>() {{
                                    put(SOLR_COLUMN_NAME, el.getString(SOLR_COLUMN_NAME));
                                    put(SOLR_COLUMN_METRIC_KEY, el.getString(SOLR_COLUMN_METRIC_KEY));
                                    put(SOLR_COLUMN_AVG, String.valueOf(el.getDouble(SOLR_COLUMN_AVG)));
                                }};

                                if (el.getDouble("chunk_avg") != 0 /*&& el.getString(CHUNK_SAX).length() == 20*/)
                                    return new ChunkWrapper(
                                            el.getString(SOLR_COLUMN_ID),
                                            el.getString(SOLR_COLUMN_SAX),
                                            tags);
                                else
                                    return null;
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    // proceed to clustering
                    ChunksClustering clustering = KMeansChunksClustering.builder()
                            .k(request.getK())
                            .maxIterations(request.getMaxIterations())
                            .distance(KMeansChunksClustering.Distance.DEFAULT)
                            .build();
                    clustering.cluster(chunkWrappers);

                    // build Json response
                    Map<String, JsonObject> clustersJson = new HashMap<>();
                    chunkWrappers.forEach(chunkClusterable -> {
                        String clusterId = chunkClusterable.getTags().get("sax_cluster");

                        // create a json cluster object if needed
                        if (!clustersJson.containsKey(clusterId)) {
                            clustersJson.put(clusterId,
                                    new JsonObject()
                                            .put("sax_cluster", clusterId)
                                            .put("cluster_size", 0)
                                            .put("chunks", new JsonArray()));
                        }

                        // add point to the cluster
                        JsonObject jsonClusterObject = clustersJson.get(clusterId);
                        jsonClusterObject.getJsonArray("chunks")
                                .add(new JsonObject()
                                        .put(SOLR_COLUMN_ID, chunkClusterable.getId())
                                        .put(SOLR_COLUMN_SAX, chunkClusterable.getSax())
                                        .put(SOLR_COLUMN_METRIC_KEY, chunkClusterable.getTags().get(SOLR_COLUMN_METRIC_KEY))
                                        .put(SOLR_COLUMN_AVG, chunkClusterable.getTags().get(SOLR_COLUMN_AVG))
                                );
                        // update size of the cluster
                        jsonClusterObject.put("cluster_size", jsonClusterObject.getJsonArray("chunks").size());
                    });

                    JsonArray clusters = new JsonArray();
                    clustersJson.values().forEach(clusters::add);

                    JsonObject finalResponse = new JsonObject()
                            .put("clustering.algo", "kmeans")
                            .put("k", request.getK())
                            .put("day", request.getDay())
                            .put("maxIterations", request.getMaxIterations())
                            .put("elapsed_time", System.currentTimeMillis() - start)
                            .put("clusters", clusters);

                    // send back the http response
                    context.response().setStatusCode(OK);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(finalResponse.encodePrettily());

                }).subscribe();
    }

    @Override
    public void dashboarding(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("dashboard generation is not implemented yet");
    }
}
