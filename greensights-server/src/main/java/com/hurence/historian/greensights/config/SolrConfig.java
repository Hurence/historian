package com.hurence.historian.greensights.config;

import com.hurence.timeseries.model.Measure;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.repository.config.EnableSolrRepositories;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Configuration
@EnableSolrRepositories( basePackages = "com.hurence.historian.greensights")
@ComponentScan
public class SolrConfig {

    @Value("#{'${historian.solr.zkHosts}'.split(',')}")
    private List<String> zkHosts;

    @Value("${historian.solr.zkChroot:}")
    private String zkChroot;

    @Value("${historian.solr.queueSize}")
    private Integer queueSize;


    @Bean
    public SolrClient solrClient() {
        if(zkChroot == null || zkChroot.isEmpty())
             return new CloudSolrClient.Builder(zkHosts, Optional.empty()).build();
        else
            return new CloudSolrClient.Builder(zkHosts, Optional.of("/solr")).build();
    }

    @Bean
    public SolrTemplate solrTemplate(SolrClient client) throws Exception {
        return new SolrTemplate(client);
    }

    @Bean
    public BlockingQueue<Measure> updateQueue(){
        return new LinkedBlockingDeque<>(queueSize);
    }

}
