package com.hurence.historian.compactor;

import static com.hurence.historian.compactor.ZkUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ZkUtilsTest {

    public static Stream<Arguments> testParseZkConnectStringInputData() {
        return Stream.of(
                // No chroot
                Arguments.of("localhost", false, Arrays.asList("localhost"), null),
                Arguments.of("localhost:2181", false, Arrays.asList("localhost:2181"), null),
                Arguments.of("foobar,foobar2", false, Arrays.asList("foobar", "foobar2"), null),
                Arguments.of("foobar:2181,foobar2:2182", false, Arrays.asList("foobar:2181", "foobar2:2182"), null),
                Arguments.of("foo.bar.fr,joe.smith.com,zk.host.domain", false, Arrays.asList("foo.bar.fr", "joe.smith.com", "zk.host.domain"), null),
                Arguments.of("foo.bar.fr,joe.smith.com:1234,zk.host.domain:5678", false, Arrays.asList("foo.bar.fr", "joe.smith.com:1234", "zk.host.domain:5678"), null),
                // With chroot
                Arguments.of("localhost/solr", false, Arrays.asList("localhost"), "/solr"),
                Arguments.of("localhost:2181/solr/something", false, Arrays.asList("localhost:2181"), "/solr/something"),
                Arguments.of("foobar,foobar2/something/else", false, Arrays.asList("foobar", "foobar2"), "/something/else"),
                Arguments.of("foobar:2181,foobar2:2182/foobar", false, Arrays.asList("foobar:2181", "foobar2:2182"), "/foobar"),
                Arguments.of("foo.bar.fr,joe.smith.com,zk.host.domain/", false, Arrays.asList("foo.bar.fr", "joe.smith.com", "zk.host.domain"), "/"),
                Arguments.of("foo.bar.fr,joe.smith.com:1234,zk.host.domain:5678/etc", false, Arrays.asList("foo.bar.fr", "joe.smith.com:1234", "zk.host.domain:5678"), "/etc"),
                // Bad
                Arguments.of(null, true, null, null),
                Arguments.of("", true, null, null),
                Arguments.of("/", true, null, null),
                Arguments.of("/foo", true, null, null));
    }

    @ParameterizedTest
    @MethodSource("testParseZkConnectStringInputData")
    public void testParseZkConnectString(String zkConnectString, boolean shouldFail,
                                         List<String> expectedHosts, String expectedChroot) {

        // Parse the zk connect string
        SolrZkConnectInfo solrZkConnectInfo = null;
        try {
            solrZkConnectInfo = ZkUtils.SolrZkConnectInfo.parseZkConnectString(zkConnectString);
        } catch(IllegalArgumentException e) {
            if (shouldFail) {
                // Expected. End test
                return;
            } else {
                fail("Parsing this zk connect string should not fail: " + zkConnectString);
            }
        }

        // Analyze expected outputs
        assertEquals(expectedHosts, solrZkConnectInfo.getZkHosts());
        if (expectedChroot != null) {
            assertEquals(expectedChroot, solrZkConnectInfo.getZkChroot());
        } else {
            assertNull(solrZkConnectInfo.getZkChroot());
        }
    }
}
