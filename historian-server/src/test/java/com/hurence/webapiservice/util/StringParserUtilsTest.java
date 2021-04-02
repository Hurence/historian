package com.hurence.webapiservice.util;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StringParserUtilsTest {

    @Test
    public void testSomeStrings(){

        assertEquals(Sets.newHashSet("a", "b"), StringParserUtils.extractTagsValues( "a$e7ea38_az!a|b$azaze12|b$aze12"));
        assertEquals(Sets.newHashSet("metric_id"), StringParserUtils.extractTagsValues("metric_id$e7ea3817-6a9a-4eab-aa56-bb8bb3c0385a"));
        assertEquals(Sets.newHashSet("metric_id", "tag2"), StringParserUtils.extractTagsValues( "metric_id$e7ea3817-6a9a-4eab-aa56-bb8bb3c0385a|tag2$azaze12"));
    }

}