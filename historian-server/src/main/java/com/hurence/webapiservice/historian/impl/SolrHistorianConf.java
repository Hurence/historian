package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.historian.compatibility.SchemaVersion;
import org.apache.solr.client.solrj.SolrClient;

public class SolrHistorianConf {
    public SolrClient client;
    public String chunkCollection;
    public String annotationCollection;
    public String streamEndPoint;
    public long limitNumberOfPoint;
    public long limitNumberOfChunks;
    public long sleepDurationBetweenTry;
    public int numberOfRetryToConnect;
    public int maxNumberOfTargetReturned;
    public SchemaVersion schemaVersion;

    public SolrHistorianConf() {
    }
// i have to add annotationcollection here
    public SolrHistorianConf(SolrClient client,
                             String collection,
                             String streamEndPoint,
                             long limitNumberOfPoint,
                             long limitNumberOfChunks,
                             long sleepDurationBetweenTry,
                             int numberOfRetryToConnect,
                             int maxNumberOfTargetReturned) {
        this.client = client;
        this.chunkCollection = collection;
        this.streamEndPoint = streamEndPoint;
        this.limitNumberOfPoint = limitNumberOfPoint;
        this.limitNumberOfChunks = limitNumberOfChunks;
        this.sleepDurationBetweenTry = sleepDurationBetweenTry;
        this.numberOfRetryToConnect = numberOfRetryToConnect;
        this.maxNumberOfTargetReturned = maxNumberOfTargetReturned;
    }


}