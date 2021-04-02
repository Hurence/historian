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

        assertEquals("U004_TC01", parameter.getName());
        assertEquals("temperature",parameter.getTags().get("type"));
        assertEquals("reacteur1_coquille1",parameter.getTags().get("sub_unit"));
        assertEquals("true",parameter.getTags().get("sample"));
        assertEquals(3,parameter.getTags().size());
    }

    @Test
    void testMatchwithPoint() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "{__name__=\"U004_TC01.test\",sample=\"true\"}");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("U004_TC01.test", parameter.getName());
        assertEquals("true",parameter.getTags().get("sample"));
        assertEquals(1,parameter.getTags().size());
    }

    @Test
    void testLuceneQuery() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "{__name__=\"U004_TC01\",sample=\"true\",sub_unit=\"reacteur1_coquille1\",type=\"temperature\"}");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("name:U004_TC01 AND sample:true AND sub_unit:reacteur1_coquille1 AND type:temperature", parameter.getLuceneQuery());
    }
}