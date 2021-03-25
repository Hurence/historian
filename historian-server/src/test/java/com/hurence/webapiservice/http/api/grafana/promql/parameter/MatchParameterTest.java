package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static com.hurence.historian.model.HistorianServiceFields.*;
class MatchParameterTest {

    @Test
    void testMatch() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "{__name__=\"U004_TC01\",sample=\"true\",sub_unit=\"reacteur1_coquille1\",type=\"temperature\"}");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("U004_TC01", parameter.getMatch().get("__name__"));
        assertEquals("temperature",parameter.getMatch().get("type"));
        assertEquals("reacteur1_coquille1",parameter.getMatch().get("sub_unit"));
        assertEquals("true",parameter.getMatch().get("sample"));
        assertEquals(4,parameter.getMatch().size());
    }

    @Test
    void testLuceneQuery() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "{__name__=\"U004_TC01\",sample=\"true\",sub_unit=\"reacteur1_coquille1\",type=\"temperature\"}");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("__name__:\"U004_TC01\" AND sample:\"true\" AND sub_unit:\"reacteur1_coquille1\" AND type:\"temperature\"", parameter.getLuceneQuery());
    }
}