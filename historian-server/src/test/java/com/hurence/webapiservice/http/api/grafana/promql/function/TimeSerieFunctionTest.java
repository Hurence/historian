package com.hurence.webapiservice.http.api.grafana.promql.function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.QueryParameter;
import com.hurence.webapiservice.http.api.grafana.util.QualityAgg;
import com.hurence.webapiservice.http.api.grafana.util.QualityConfig;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.historian.model.HistorianServiceFields.QUALITY_AGG;
import static com.hurence.historian.model.HistorianServiceFields.QUALITY_VALUE;
import static org.junit.jupiter.api.Assertions.*;

class TimeSerieFunctionTest {


    private long START_CHUNK = 1477916224865L;
    private long END_CHUNK = 1477916224873L;


    JsonArray getPoints() {

        final List<Chunk> chunks = new ArrayList<>();
        chunks.add(
                ChunkBuilderHelper.fromPointsAndTags(
                        "energy_impact_kwh",
                        Arrays.asList(
                                Measure.fromValue(START_CHUNK, 1),
                                Measure.fromValue(1477916224866L, 1),
                                Measure.fromValue(1477916224867L, 1),
                                Measure.fromValue(1477916224868L, 1),
                                Measure.fromValue(1477916224869L, 1),
                                Measure.fromValue(1477916224871L, 1),
                                Measure.fromValue(1477916224872L, 1),
                                Measure.fromValue(END_CHUNK, 1)),
                        Maps.newHashMap( ImmutableMap.of("account_id", "93090364", "country", "france"))
                )
        );


        chunks.add(
                ChunkBuilderHelper.fromPointsAndTags(
                        "energy_impact_kwh",
                        Arrays.asList(
                                Measure.fromValue(START_CHUNK, 2),
                                Measure.fromValue(1477916224871L, 2),
                                Measure.fromValue(END_CHUNK, 2)
                        ),
                        Maps.newHashMap(ImmutableMap.of("account_id", "93090364", "country", "canada")))
        );

        chunks.add(
                ChunkBuilderHelper.fromPointsAndTags(
                        "energy_impact_kwh",
                        Arrays.asList(
                                Measure.fromValue(1477916224869L, 3),
                                Measure.fromValue(1477916224870L, 3),
                                Measure.fromValue(END_CHUNK, 3)
                        ),
                        Maps.newHashMap(ImmutableMap.of("account_id", "93090364", "country", "europe")))
        );


        JsonArray points = new JsonArray();
        chunks.forEach( chunk -> {

            JsonArray dataPoints = new JsonArray();
            try {
                chunk.getValueAsMeasures().forEach(measure -> {
                    JsonArray point = new JsonArray();
                    point.add(measure.getValue()).add(measure.getTimestamp());
                    dataPoints.add(point);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }

            JsonObject result = new JsonObject()
                    .put("name", chunk.getName())
                    .put("tags", chunk.getTags())
                    .put("total_points", chunk.getCount())
                    .put("datapoints", dataPoints);
            points.add(result);
        });

        return points;
    }

    @Test
    void testSumFunction() {
        String query = "sum(energy_impact_kwh{account_id=\"93090364\"})";

        QueryParameter queryParameter = QueryParameter.builder().parse(query).build();


        final JsonArray points = getPoints();

        TimeseriesFunction function = TimeserieFunctionFactory.getInstance(queryParameter.getAggregationOperator().get());
        final JsonObject result = function.process(points).getJsonObject(0);

        assertEquals(4, result.size());
        assertEquals("energy_impact_kwh", result.getString("name"));
        assertEquals(9, result.getInteger("total_points"));

        final JsonArray datapoints = result.getJsonArray("datapoints");
        assertEquals(9, datapoints.size());
        assertEquals(3.0, datapoints.getJsonArray(0).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(1).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(2).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(3).getDouble(0));
        assertEquals(4.0, datapoints.getJsonArray(4).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(5).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(6).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(7).getDouble(0));
        assertEquals(6.0, datapoints.getJsonArray(8).getDouble(0));

        assertEquals(1477916224865L, datapoints.getJsonArray(0).getLong(1));
        assertEquals(1477916224866L, datapoints.getJsonArray(1).getLong(1));
        assertEquals(1477916224867L, datapoints.getJsonArray(2).getLong(1));
        assertEquals(1477916224868L, datapoints.getJsonArray(3).getLong(1));
        assertEquals(1477916224869L, datapoints.getJsonArray(4).getLong(1));
        assertEquals(1477916224870L, datapoints.getJsonArray(5).getLong(1));
        assertEquals(1477916224871L, datapoints.getJsonArray(6).getLong(1));
        assertEquals(1477916224872L, datapoints.getJsonArray(7).getLong(1));
        assertEquals(1477916224873L, datapoints.getJsonArray(8).getLong(1));

    }


    @Test
    void testAvgFunction() {
        String query = "avg(energy_impact_kwh{account_id=\"93090364\"})";

        QueryParameter queryParameter = QueryParameter.builder().parse(query).build();


        final JsonArray points = getPoints();

        TimeseriesFunction function = TimeserieFunctionFactory.getInstance(queryParameter.getAggregationOperator().get());
        final JsonObject result = function.process(points).getJsonObject(0);

        assertEquals(4, result.size());
        assertEquals("energy_impact_kwh", result.getString("name"));
        assertEquals(9, result.getInteger("total_points"));

        final JsonArray datapoints = result.getJsonArray("datapoints");
        assertEquals(9, datapoints.size());
        assertEquals(1.5, datapoints.getJsonArray(0).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(1).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(2).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(3).getDouble(0));
        assertEquals(2.0, datapoints.getJsonArray(4).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(5).getDouble(0));
        assertEquals(1.5, datapoints.getJsonArray(6).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(7).getDouble(0));
        assertEquals(2.0, datapoints.getJsonArray(8).getDouble(0));

        assertEquals(1477916224865L, datapoints.getJsonArray(0).getLong(1));
        assertEquals(1477916224866L, datapoints.getJsonArray(1).getLong(1));
        assertEquals(1477916224867L, datapoints.getJsonArray(2).getLong(1));
        assertEquals(1477916224868L, datapoints.getJsonArray(3).getLong(1));
        assertEquals(1477916224869L, datapoints.getJsonArray(4).getLong(1));
        assertEquals(1477916224870L, datapoints.getJsonArray(5).getLong(1));
        assertEquals(1477916224871L, datapoints.getJsonArray(6).getLong(1));
        assertEquals(1477916224872L, datapoints.getJsonArray(7).getLong(1));
        assertEquals(1477916224873L, datapoints.getJsonArray(8).getLong(1));

    }


    @Test
    void testCountFunction() {
        String query = "count(energy_impact_kwh{account_id=\"93090364\"})";

        QueryParameter queryParameter = QueryParameter.builder().parse(query).build();


        final JsonArray points = getPoints();

        TimeseriesFunction function = TimeserieFunctionFactory.getInstance(queryParameter.getAggregationOperator().get());
        final JsonObject result = function.process(points).getJsonObject(0);

        assertEquals(4, result.size());
        assertEquals("energy_impact_kwh", result.getString("name"));
        assertEquals(9, result.getInteger("total_points"));

        final JsonArray datapoints = result.getJsonArray("datapoints");
        assertEquals(9, datapoints.size());
        assertEquals(2, datapoints.getJsonArray(0).getDouble(0));
        assertEquals(1, datapoints.getJsonArray(1).getDouble(0));
        assertEquals(1, datapoints.getJsonArray(2).getDouble(0));
        assertEquals(1, datapoints.getJsonArray(3).getDouble(0));
        assertEquals(2, datapoints.getJsonArray(4).getDouble(0));
        assertEquals(1, datapoints.getJsonArray(5).getDouble(0));
        assertEquals(2, datapoints.getJsonArray(6).getDouble(0));
        assertEquals(1, datapoints.getJsonArray(7).getDouble(0));
        assertEquals(3, datapoints.getJsonArray(8).getDouble(0));

        assertEquals(1477916224865L, datapoints.getJsonArray(0).getLong(1));
        assertEquals(1477916224866L, datapoints.getJsonArray(1).getLong(1));
        assertEquals(1477916224867L, datapoints.getJsonArray(2).getLong(1));
        assertEquals(1477916224868L, datapoints.getJsonArray(3).getLong(1));
        assertEquals(1477916224869L, datapoints.getJsonArray(4).getLong(1));
        assertEquals(1477916224870L, datapoints.getJsonArray(5).getLong(1));
        assertEquals(1477916224871L, datapoints.getJsonArray(6).getLong(1));
        assertEquals(1477916224872L, datapoints.getJsonArray(7).getLong(1));
        assertEquals(1477916224873L, datapoints.getJsonArray(8).getLong(1));

    }

    @Test
    void testMaxFunction() {
        String query = "max(energy_impact_kwh{account_id=\"93090364\"})";

        QueryParameter queryParameter = QueryParameter.builder().parse(query).build();


        final JsonArray points = getPoints();

        TimeseriesFunction function = TimeserieFunctionFactory.getInstance(queryParameter.getAggregationOperator().get());
        final JsonObject result = function.process(points).getJsonObject(0);

        assertEquals(4, result.size());
        assertEquals("energy_impact_kwh", result.getString("name"));
        assertEquals(9, result.getInteger("total_points"));

        final JsonArray datapoints = result.getJsonArray("datapoints");
        assertEquals(9, datapoints.size());
        assertEquals(2.0, datapoints.getJsonArray(0).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(1).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(2).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(3).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(4).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(5).getDouble(0));
        assertEquals(2.0, datapoints.getJsonArray(6).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(7).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(8).getDouble(0));

        assertEquals(1477916224865L, datapoints.getJsonArray(0).getLong(1));
        assertEquals(1477916224866L, datapoints.getJsonArray(1).getLong(1));
        assertEquals(1477916224867L, datapoints.getJsonArray(2).getLong(1));
        assertEquals(1477916224868L, datapoints.getJsonArray(3).getLong(1));
        assertEquals(1477916224869L, datapoints.getJsonArray(4).getLong(1));
        assertEquals(1477916224870L, datapoints.getJsonArray(5).getLong(1));
        assertEquals(1477916224871L, datapoints.getJsonArray(6).getLong(1));
        assertEquals(1477916224872L, datapoints.getJsonArray(7).getLong(1));
        assertEquals(1477916224873L, datapoints.getJsonArray(8).getLong(1));

    }

    @Test
    void testMinFunction() {
        String query = "min(energy_impact_kwh{account_id=\"93090364\"})";

        QueryParameter queryParameter = QueryParameter.builder().parse(query).build();


        final JsonArray points = getPoints();

        TimeseriesFunction function = TimeserieFunctionFactory.getInstance(queryParameter.getAggregationOperator().get());
        final JsonObject result = function.process(points).getJsonObject(0);

        assertEquals(4, result.size());
        assertEquals("energy_impact_kwh", result.getString("name"));
        assertEquals(9, result.getInteger("total_points"));

        final JsonArray datapoints = result.getJsonArray("datapoints");
        assertEquals(9, datapoints.size());
        assertEquals(1.0, datapoints.getJsonArray(0).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(1).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(2).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(3).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(4).getDouble(0));
        assertEquals(3.0, datapoints.getJsonArray(5).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(6).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(7).getDouble(0));
        assertEquals(1.0, datapoints.getJsonArray(8).getDouble(0));

        assertEquals(1477916224865L, datapoints.getJsonArray(0).getLong(1));
        assertEquals(1477916224866L, datapoints.getJsonArray(1).getLong(1));
        assertEquals(1477916224867L, datapoints.getJsonArray(2).getLong(1));
        assertEquals(1477916224868L, datapoints.getJsonArray(3).getLong(1));
        assertEquals(1477916224869L, datapoints.getJsonArray(4).getLong(1));
        assertEquals(1477916224870L, datapoints.getJsonArray(5).getLong(1));
        assertEquals(1477916224871L, datapoints.getJsonArray(6).getLong(1));
        assertEquals(1477916224872L, datapoints.getJsonArray(7).getLong(1));
        assertEquals(1477916224873L, datapoints.getJsonArray(8).getLong(1));

    }
}