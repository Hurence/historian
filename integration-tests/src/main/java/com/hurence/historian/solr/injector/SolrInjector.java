package com.hurence.historian.solr.injector;

import com.hurence.historian.modele.SchemaVersion;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

public interface SolrInjector {

    SchemaVersion getVersion();

    void injectChunks(SolrClient client) throws SolrServerException, IOException;

}
