/**
 * Copyright (C) 2019 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.unit5.extensions;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.util.*;

/**
 * A JUnit rule which starts an embedded elastic-search docker container.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class SolrExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private static Logger logger = LoggerFactory.getLogger(SolrExtension.class);
    public final static String SOLR2_SERVICE_NAME = "solr2_1";
    public final static String SOLR1_SERVICE_NAME = "solr1_1";
    public final static int SOLR_1_PORT = 8983;
    public final static int SOLR_2_PORT = 8983;
    public final static String ZOOKEEPER_SERVICE_NAME = "zookeeper_1";
    public final static int ZOOKEEPER_PORT = 2181;
    private final static String IMAGE = "solr:8";
    public final static String SOLR_CONF_TEMPLATE_HISTORIAN_CURRENT = "historian-current";
    public final static String SOLR_CONF_TEMPLATE_HISTORIAN_VERSION_0 = "historian-version-0";
    public final static String SOLR_CONF_TEMPLATE_ANNOTATION = "annotation";
    public final static String SOLR_CONF_TEMPLATE_REPORT = "report";
    public final static String SOLR_CONF_TEMPLATE_DEFAULT = "_default";
    private static final HashSet<Class> INJECTABLE_TYPES = new HashSet<Class>() {
        {
            add(SolrClient.class);
            add(DockerComposeContainer.class);
        }
    };
    /**
     * The internal-transport client that talks to the local node.
     */
    private SolrClient client;
    private DockerComposeContainer dockerComposeContainer;

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public SolrClient getClient() {
        return client;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (getClient() != null) getClient().close();
        if (dockerComposeContainer != null) dockerComposeContainer.stop();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        this.dockerComposeContainer = new DockerComposeContainer(
                new File(getClass().getResource("/shared-resources/docker-compose-test.yml").getFile())
        )
                .withExposedService(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT, Wait.forListeningPort())
                .withExposedService(SOLR1_SERVICE_NAME, SOLR_1_PORT, Wait.forListeningPort())
                .waitingFor(SOLR2_SERVICE_NAME, Wait.forListeningPort());

        this.dockerComposeContainer.start();

        String zkUrl = getZkUrl(dockerComposeContainer);
        logger.info("url of zookeeper http://" + zkUrl);

        String slrUrl = getSolr1Url(dockerComposeContainer);
        logger.info("url of solr http://" + slrUrl);

        CloudSolrClient.Builder clientBuilder = new CloudSolrClient.Builder(
                Arrays.asList(zkUrl),
                    Optional.empty());

        this.client = clientBuilder
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return INJECTABLE_TYPES.contains(parameterType(parameterContext));
    }

    private Class<?> parameterType(ParameterContext parameterContext) {
        return parameterContext.getParameter().getType();
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterType(parameterContext);
        if (type == SolrClient.class) {
            return getClient();
        }
        if (type == DockerComposeContainer.class) {
            return dockerComposeContainer;
        }
//        if (type == GenericContainer.class) {
//            return container;
//        }
//        if (type == Container.class) {
//            return container;
//        }
        throw new IllegalStateException("Looks like the ParameterResolver needs a fix...");
    }

    private static Logger getLogger() {
        return logger;
    }

    public static String getZkUrl(DockerComposeContainer dockerComposeContainer) {
        return dockerComposeContainer.getServiceHost(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT)
                + ":" +
                dockerComposeContainer.getServicePort(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT);
    }

    public static String getSolr1Url(DockerComposeContainer dockerComposeContainer) {
        return dockerComposeContainer.getServiceHost(SOLR1_SERVICE_NAME, SOLR_1_PORT)
                + ":" +
                dockerComposeContainer.getServicePort(SOLR1_SERVICE_NAME, SOLR_1_PORT);
    }
}