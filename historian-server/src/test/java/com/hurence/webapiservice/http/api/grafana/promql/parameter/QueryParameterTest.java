package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.google.common.collect.ImmutableMap;

import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;
import org.junit.jupiter.api.Test;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static com.hurence.historian.model.HistorianServiceFields.*;


class QueryParameterTest {

    @Test
    void queryWithOperatorWithTags() {
        Map<String, String> query = ImmutableMap.of(
                QUERY,
                "min(U004_TC01{type=\"temperature\", sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12\"})");

        QueryParameter parameter = QueryParameter.builder()
                .parameters(query)
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
        Map<String, String> query1 = ImmutableMap.of(
                QUERY,
                "min(U004_TC01{type=\"temperature\", sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12})");

        QueryParameter parameter1 = QueryParameter.builder()
                .parameters(query1)
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
        Map<String, String> query = ImmutableMap.of(QUERY, "min(U004_TC01)");
        QueryParameter parameter = QueryParameter.builder()
                .parameters(query)
                .build();

        assertTrue(parameter.getAggregationOperator().isPresent());
        assertEquals("min", parameter.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter.getName());
        assertEquals(0, parameter.getTags().size());

        Map<String, String> query1 = ImmutableMap.of(QUERY, "min(U004_TC01{})");
        QueryParameter parameter1 = QueryParameter.builder()
                .parameters(query1)
                .build();

        assertTrue(parameter1.getAggregationOperator().isPresent());
        assertEquals("min", parameter1.getAggregationOperator().get().label);
        assertEquals("U004_TC01", parameter1.getName());
        assertEquals(0, parameter1.getTags().size());
    }

    @Test
    void queryWithoutOperator() {
        Map<String,String> query = ImmutableMap.of(
                QUERY,
                "U004_TC01{type=\"temperature\", sub_unit=\"reacteur1_coquille1\", sample=\"true\", bucket=\"12\"}");
        QueryParameter parameter = QueryParameter.builder()
                .parameters(query)
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
        Map<String,String> query = ImmutableMap.of(                QUERY,                "U004_TC01{}");
        QueryParameter parameter = QueryParameter.builder()
                .parameters(query)
                .build();

        assertFalse(parameter.getAggregationOperator().isPresent());
        assertEquals("U004_TC01", parameter.getName());
        assertEquals(0, parameter.getTags().size());

        Map<String,String> query1 = ImmutableMap.of(                QUERY,                "U004_TC01");
        QueryParameter parameter1 = QueryParameter.builder()
                .parameters(query1)
                .build();

        assertFalse(parameter1.getAggregationOperator().isPresent());
        assertEquals("U004_TC01", parameter1.getName());
        assertEquals(0, parameter1.getTags().size());
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
}