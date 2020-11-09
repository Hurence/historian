package com.hurence.historian.compactor;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for Zookeeper related functions
 */
public class ZkUtils {

    /**
     * Connection information suitable for the CloudSolrClient.Builder
     * constructor
     */
    public static class SolrZkConnectInfo {

        // List of host:[port] information for zk hosts
        private List<String> zkHosts = new ArrayList<String>();
        // Path to root znode for solr configuration
        private String zkChroot = null;

        /**
         * Private to force parsing method usage
         */
        private SolrZkConnectInfo() {}

        public List<String> getZkHosts() {
            return zkHosts;
        }

        public String getZkChroot() {
            return zkChroot;
        }

        /**
         * Parses a solr zk connectString as specified in
         * https://zookeeper.apache.org/doc/r3.4.13/api/org/apache/zookeeper/ZooKeeper.html
         * to return a SolrZkConnectInfo suitable for hosting needed information
         * to connect to Solr Cloud through ZK, using CloudSolrClient.Builder
         * @return
         */
        public static SolrZkConnectInfo parseZkConnectString(String zkConnectString) {

            if ((zkConnectString == null) || zkConnectString.length() == 0) {
                throw new IllegalArgumentException("Null of empty connect string");
            }

            zkConnectString = zkConnectString.trim();

            SolrZkConnectInfo solrZkConnectInfo = new SolrZkConnectInfo();

            /**
             * If some root path given, parse it
             */

            int slashIndex = zkConnectString.indexOf("/");
            if (slashIndex != -1) {
                if (slashIndex < 1) {
                    throw new IllegalArgumentException("Missing zk hosts in: " + zkConnectString);
                }
                // '/' character is present, extract solr root path from this first character.
                solrZkConnectInfo.zkChroot = zkConnectString.substring(slashIndex);

                // Let left part being parsed
                zkConnectString = zkConnectString.substring(0, slashIndex);
            }

            /**
             * Parse hosts
             */

            String[] hosts = zkConnectString.split(",");
            for (String host : hosts) {
                solrZkConnectInfo.zkHosts.add(host);
            }

            return solrZkConnectInfo;
        }

        public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }
    }
}
