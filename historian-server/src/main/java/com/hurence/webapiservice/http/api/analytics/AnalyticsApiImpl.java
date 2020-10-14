package com.hurence.webapiservice.http.api.analytics;

import com.google.gson.stream.JsonReader;
import com.hurence.historian.util.ErrorMsgHelper;
import com.hurence.timeseries.analysis.clustering.ChunkClusterable;
import com.hurence.timeseries.analysis.clustering.ChunksClustering;
import com.hurence.timeseries.analysis.clustering.KMeansChunksClustering;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.analytics.model.ChunkWrapper;
import com.hurence.webapiservice.http.api.analytics.model.ClusteringRequest;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginAnnotationRequestParser;
import com.hurence.webapiservice.http.api.ingestion.IngestionApiImpl;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import io.vertx.reactivex.ext.web.RoutingContext;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.util.JsonRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hurence.historian.model.FieldNamesInsideHistorianService.CHUNK_SAX;
import static com.hurence.historian.model.HistorianServiceFields.POINTS;
import static com.hurence.historian.model.HistorianServiceFields.TIMESERIES;
import static com.hurence.timeseries.model.Definitions.SOLR_COLUMN_ORIGIN;
import static com.hurence.webapiservice.http.api.ingestion.util.IngestionFinalResponseUtil.constructFinalResponseCsv;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.*;

public class AnalyticsApiImpl implements AnalyticsApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiImpl.class);
    private HistorianService service;

    private JsonArray docs;

    public AnalyticsApiImpl(HistorianService service) {
        this.service = service;

        InputStream saxFile = getClass().getResourceAsStream("/sax-samples.json");
        try {
            String jsonStr = IOUtils.toString(saxFile);
            docs = new JsonObject(jsonStr)
                    .getJsonObject("response").getJsonArray("docs");
        } catch (IOException exception) {
            exception.printStackTrace();
        }


    }


    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian analytics api is Working fine, try out /clustering");
    }

    @Override
    public void clustering(RoutingContext context) {

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




        service.rxGetTimeSeriesChunk(request.toParams())
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "text/plain");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(response -> {

                    ChunksClustering clustering = KMeansChunksClustering.builder()
                            .k(request.getK())
                            .maxIterations(request.getMaxIterations())
                            .distance(KMeansChunksClustering.Distance.DEFAULT)
                            .build();


                    List<ChunkClusterable> chunkWrappers = response.getJsonArray("chunks").stream()
                            .map(metric -> {
                                JsonObject el = (JsonObject) metric;



                                Map<String, String> tags = new HashMap<String, String>() {{
                                    put("name", el.getString("name"));
                                    put("metric_id", el.getString("metric_id"));
                                    put("avg", String.valueOf(el.getDouble("chunk_avg")));
                                }};

                                if(el.getDouble("chunk_avg") != 0 && el.getString(CHUNK_SAX).length() ==20)
                                return new ChunkWrapper(
                                        el.getString("id"),
                                        el.getString(CHUNK_SAX),
                                        tags);
                                else
                                    return null;
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());


                    clustering.cluster(chunkWrappers);

                    JsonArray results = new JsonArray();
                    chunkWrappers.forEach( chunkClusterable -> {
                        JsonObject o = new JsonObject()
                                .put("id", chunkClusterable.getId())
                                .put("chunk_sax", chunkClusterable.getSax())
                                .put( "tags", chunkClusterable.getTags());
                        results.add(o);
                    });

                    JsonObject finalResponse = new JsonObject()
                            .put("results", results)
                            .put("clustering.algo", "kmeans");

                    context.response().setStatusCode(OK);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(finalResponse.encodePrettily());

                }).subscribe();
    }

    @Override
    public void dashboarding(RoutingContext context) {

    }
}
