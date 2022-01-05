package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.google.common.collect.ImmutableMap;

import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.promql.function.TimeserieFunctionType;
import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;
import org.junit.jupiter.api.Test;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static com.hurence.historian.model.HistorianServiceFields.*;


class QueryParameterTest {

    @Test
    void queryWithOperatorWithTags() {
        String query = "min(U004_TC01{type=\"temperature\", sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12\"})";

        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertTrue(parameter.getAggregationOperator().isPresent());
        assertEquals("min", parameter.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter.getName());
        assertEquals("temperature", parameter.getTags().get("type"));
        assertEquals("reacteur1_coquille1", parameter.getTags().get("sub_unit"));
        assertEquals("true", parameter.getTags().get("sample"));
        assertEquals("12", parameter.getTags().get("bucket"));
        assertEquals(4, parameter.getTags().size());
    }

    @Test
    void queryWithOperatorWithTagsNoQuotes() {
        String query = "min(U004_TC01{type=temperature, sub_unit=reacteur1_coquille1, sample=true, bucket=12})";

        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertTrue(parameter.getAggregationOperator().isPresent());
        assertEquals("min", parameter.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter.getName());
        assertEquals("temperature", parameter.getTags().get("type"));
        assertEquals("reacteur1_coquille1", parameter.getTags().get("sub_unit"));
        assertEquals("true", parameter.getTags().get("sample"));
        assertEquals("12", parameter.getTags().get("bucket"));
        assertEquals(4, parameter.getTags().size());
    }


    @Test
    void queryWithOperatorWithTagsMalformed() {
        String query = "min(U004_TC01{type=\"temperature\", sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12})";

        QueryParameter parameter1 = QueryParameter.builder()
                .parse(query)
                .build();

        assertTrue(parameter1.getAggregationOperator().isPresent());
        assertEquals("min", parameter1.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter1.getName());
        assertEquals("temperature", parameter1.getTags().get("type"));
        assertEquals("reacteur1_coquille1", parameter1.getTags().get("sub_unit"));
        assertEquals("true", parameter1.getTags().get("sample"));
        assertEquals("12", parameter1.getTags().get("bucket"));
        assertEquals(4, parameter1.getTags().size());
    }

    @Test
    void queryWithOperatorWithoutTags() {
        String query = "min(U004_TC01)";
        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertTrue(parameter.getAggregationOperator().isPresent());
        assertEquals("min", parameter.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter.getName());
        assertEquals(0, parameter.getTags().size());

        String query1 = "min(U004_TC01{})";
        QueryParameter parameter1 = QueryParameter.builder()
                .parse(query1)
                .build();

        assertTrue(parameter1.getAggregationOperator().isPresent());
        assertEquals("min", parameter1.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter1.getName());
        assertEquals(0, parameter1.getTags().size());
    }

    @Test
    void queryWithoutOperator() {
        String query = "U004_TC01{type=\"temperature\", sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12\"}";
        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertFalse(parameter.getAggregationOperator().isPresent());
        assertEquals("U004_TC01", parameter.getName());
        assertEquals("temperature", parameter.getTags().get("type"));
        assertEquals("reacteur1_coquille1", parameter.getTags().get("sub_unit"));
        assertEquals("true", parameter.getTags().get("sample"));
        assertEquals("12", parameter.getTags().get("bucket"));
        assertEquals(4, parameter.getTags().size());
    }

    @Test
    void queryWithoutOperatorNoComma() {
        String query = "U004_TC01{type=\"temperature\" sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12\"}";
        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertFalse(parameter.getAggregationOperator().isPresent());
        assertEquals("U004_TC01", parameter.getName());
        assertEquals("temperature", parameter.getTags().get("type"));
        assertEquals("reacteur1_coquille1", parameter.getTags().get("sub_unit"));
        assertEquals("true", parameter.getTags().get("sample"));
        assertEquals("12", parameter.getTags().get("bucket"));
        assertEquals(4, parameter.getTags().size());
    }

    @Test
    void queryWithoutOperatorWithoutTags() {
        String query = "U004_TC01{}";
        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertFalse(parameter.getAggregationOperator().isPresent());
        assertEquals("U004_TC01", parameter.getName());
        assertEquals(0, parameter.getTags().size());

        String query1 = "U004_TC01";
        QueryParameter parameter1 = QueryParameter.builder()
                .parse(query1)
                .build();

        assertFalse(parameter1.getAggregationOperator().isPresent());
        assertEquals("U004_TC01", parameter1.getName());
        assertEquals(0, parameter1.getTags().size());
    }

    @Test
    void queryWithSampling() {
        String query = "T473.SC02_OP.F_CV{ measure=\"aze\", sampling_algo=\"MIN\", bucket_size=\"100\" }";
        QueryParameter parameter = QueryParameter.builder()
                .parse(query)
                .build();

        assertFalse(parameter.getAggregationOperator().isPresent());
        assertEquals("T473.SC02_OP.F_CV", parameter.getName());
        assertEquals("aze", parameter.getTags().get("measure"));
        assertEquals(1, parameter.getTags().size());

        assertEquals(SamplingAlgorithm.MIN, parameter.getSampling().getAlgo());
        assertEquals(100, parameter.getSampling().getBucketSize());

        String query2 = "T473.SC02_OP.F_CV{ measure=\"aze\", sampling_algo=\"MINz\", bucket_size=\"100\" }";
        QueryParameter parameter2 = QueryParameter.builder()
                .parse(query2)
                .build();

        assertEquals(SamplingAlgorithm.NONE, parameter2.getSampling().getAlgo());
        assertEquals(100, parameter2.getSampling().getBucketSize());

        String query3 = "T473.SC02_OP.F_CV{  sampling_algo=\"max\" }";
        QueryParameter parameter3 = QueryParameter.builder()
                .parse(query3)
                .build();

        assertEquals(SamplingAlgorithm.MAX, parameter3.getSampling().getAlgo());
    }


    @Test
    void queryErrors() {
        Map<String, String> query = ImmutableMap.of(
                QUERY,
                "U004_TC01",
                TIME,
                "f12f"
        );

        QueryRequest request = QueryRequest.builder()
                .parameters(query)
                .build();

        assertFalse(request.getErrors().error.isEmpty());
    }


    @Test
    void testRangeDuration() {

        String query = "abs(node_cpu_seconds_total{ measure=\"aze\", sampling_algo=\"MIN\"}[1h])";
        RangeDuration rangeDuration = RangeDuration.builder().parse(query).build();

        QueryParameter queryParameter = QueryParameter.builder().parse(query).build();

        assertEquals(SamplingAlgorithm.MIN, queryParameter.getSampling().getAlgo());
        assertEquals(1, queryParameter.getSampling().getBucketSize());
        assertEquals(Duration.ofHours(1), queryParameter.getRangeDuration().getDuration());
        assertEquals(TimeserieFunctionType.ABS, queryParameter.getAggregationOperator().get());
        assertEquals("aze", queryParameter.getTags().get("measure"));
    }

    @Test
    void multiplePromQLQueries() {

        List<String> queries = Arrays.asList(
                "node_cpu_seconds_total",
                "node_cpu_seconds_total[5m]",
                "node_cpu_seconds_total{cpu=\"15\",mode=\"idle\"}",
                "node_cpu_seconds_total{cpu=\"15\",mode=\"idle\"}[5m]",
                "sum(node_cpu_seconds_total{cpu=\"45\",mode=\"idle\"}[5m])",
                "node_cpu_seconds_total{cpu!=\"0\",mode=~\"user|system\"}",
                "sum(node_cpu_seconds_total)",
                "rate(node_cpu_seconds_total[5m])",
                "increase(node_cpu_seconds_total[1h])"/*,
                "node_cpu_seconds_total offset 1d",
                "sum by(job, instance) (node_cpu_seconds_total)",
                "sum without(instance, job) (node_cpu_seconds_total)"*/
        );

        List<QueryParameter> queryParameters = queries.stream()
                .map(q -> QueryParameter.builder().parse(q).build())
                .collect(Collectors.toList());
        int size = queries.size();

        String METRIC_NAME = "node_cpu_seconds_total";
        for (int i = 0; i < size; i++) {
            assertEquals(METRIC_NAME, queryParameters.get(i).getName());
        }

    }


    @Test
    void testSumBy() {

        List<String> queries = Arrays.asList(
                "sum by(measure, cotation) (metric{ measure=\"aze\", cotation=\"8a\"}[1h])",
                "sum(metric{ measure=\"aze\", cotation=\"8a\"}[1h]) by (measure, cotation)",
                "sum( metric{measure=\"aze\", cotation=\"8a\"}[1h]) by(measure,cotation)");

        for (String query : queries) {

            QueryParameter queryParameter = QueryParameter.builder().parse(query).build();

            assertEquals("metric", queryParameter.getName());
            assertEquals(TimeserieFunctionType.SUM, queryParameter.getAggregationOperator().get());
            assertEquals("aze", queryParameter.getTags().get("measure"));
            assertEquals("8a", queryParameter.getTags().get("cotation"));

            assertEquals(2, queryParameter.getGroupByParameter().getNames().size());
            assertEquals("measure", queryParameter.getGroupByParameter().getNames().get(0));
            assertEquals("cotation", queryParameter.getGroupByParameter().getNames().get(1));

        }

    }


}