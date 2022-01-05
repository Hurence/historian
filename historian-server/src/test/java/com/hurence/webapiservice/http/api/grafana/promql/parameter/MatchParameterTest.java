package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static com.hurence.historian.model.HistorianServiceFields.*;
class MatchParameterTest {


    @Test
    void testSimpleMatch() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "U004_TC01{ sample=\"true\", sub_unit=\"reacteur1_coquille1\", type=\"temperature\"}");

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
    void testSimpleMatchNoTags() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "U004_TC01{ }");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("U004_TC01", parameter.getName());
        assertEquals(0,parameter.getTags().size());
    }

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

        assertEquals("name:U004_TC01 AND sample:\"true\" AND sub_unit:\"reacteur1_coquille1\" AND type:\"temperature\"", parameter.getLuceneQuery());
    }

    @Test
    void testLuceneQueryWithWildcards() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "{__name__=\"U004_TC01\",sample=\"true\",sub_unit=~\".*\",type=\"temperature\"}");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("name:U004_TC01 AND sample:\"true\" AND sub_unit:\"*\" AND type:\"temperature\"", parameter.getLuceneQuery());


        Map<String, String> query2 = ImmutableMap.of(
                MATCH,
                "{__name__=\"U004_TC01\",sample=\"true\",sub_unit=~\".+\",type=\"temperature\"}");

        MatchParameter parameter2 = MatchParameter.builder()
                .parameters(query2)
                .build();

        assertEquals("name:U004_TC01 AND sample:\"true\" AND sub_unit:\"*\" AND type:\"temperature\"", parameter2.getLuceneQuery());

    }

    @Test
    void testLuceneQuery2() {

        Map<String, String> query = ImmutableMap.of(
                MATCH,
                "solr_collections_shard_state{zk_host=~\".*\",collection=~\".+\"}");

        MatchParameter parameter = MatchParameter.builder()
                .parameters(query)
                .build();

        assertEquals("name:solr_collections_shard_state AND zk_host:\"*\" AND collection:\"*\"", parameter.getLuceneQuery());


    }

}