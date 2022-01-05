package com.hurence.historian.greensights.repository;

import com.hurence.historian.greensights.model.solr.WebPageActivityAnalysis;
import org.springframework.data.solr.repository.SolrCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WebPageActivityAnalysisRepository extends SolrCrudRepository<WebPageActivityAnalysis, String> {

}
