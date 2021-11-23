package com.hurence.historian.scraper.solr;

import com.hurence.timeseries.model.Measure;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Configuration
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
    public BlockingQueue<Measure> updateQueue(){
        return new LinkedBlockingDeque<>(queueSize);
    }



}
