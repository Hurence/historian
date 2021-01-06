package com.hurence.webapiservice.http.api.main;

import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.modele.MultiMapRequestParser;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.reactivex.core.MultiMap;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.api.main.modele.QueryFields.*;

public class GetTimeSerieRequestParser extends MultiMapRequestParser {

    public GetTimeSerieRequestParam parseRequest(MultiMap map) throws IllegalArgumentException {
        GetTimeSerieRequestParam.Builder builder = new GetTimeSerieRequestParam.Builder();
        long from = parseLong(map, QUERY_PARAM_FROM);
        builder.from(from);
        long to = parseLongOrDefault(map, QUERY_PARAM_TO, Long.MAX_VALUE);
        builder.to(to);
        List<AGG> aggs = parseAggsOrDefault(map, QUERY_PARAM_AGGS, Collections.emptyList());
        builder.withAggs(aggs);
        int maxPoints = parseIntOrDefault(map, QUERY_PARAM_MAX_POINT, 10000);
        builder.withMaxDataPoints(maxPoints);
        int bucketSize = parseIntOrDefault(map, QUERY_PARAM_BUCKET_SIZE, 50);
        builder.withBucketSize(bucketSize);
        SamplingAlgorithm algo = parseSamplingAlgorithmOrDefault(map, QUERY_PARAM_SAMPLING, SamplingAlgorithm.NONE);
        builder.withSamplingAlgo(algo);
        List<String> names = parseListOrDefault(map, QUERY_PARAM_NAME, Collections.emptyList());
        builder.withMetricNames(names);
        return builder.build();
    }

    private SamplingAlgorithm parseSamplingAlgorithmOrDefault(MultiMap map, String queryParam, SamplingAlgorithm defaut) {
        if (map.contains(queryParam)) {
            return SamplingAlgorithm.valueOf(map.get(QUERY_PARAM_SAMPLING));
        } else {
            return defaut;
        }
    }


    private List<AGG> parseAggsOrDefault(MultiMap map, String queryParam, List<AGG> defaut) {
        if (map.contains(queryParam)) {
            return parseAggs(map, queryParam);
        } else {
            return defaut;
        }
    }


    private List<AGG> parseAggs(MultiMap map, String queryParam) throws IllegalArgumentException {
        try {
            return map.getAll(queryParam).stream()
                    .map(AGG::valueOf)//TODO more robust ?
                    .collect(Collectors.toList());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Could not parse parameter '%s' as a list of aggs.",
                            queryParam), ex
            );
        }
    }
}
