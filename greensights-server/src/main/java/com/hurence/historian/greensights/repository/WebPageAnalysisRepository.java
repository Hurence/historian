package com.hurence.historian.greensights.repository;

import com.hurence.historian.greensights.model.solr.WebPageAnalysis;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.solr.repository.Query;
import org.springframework.data.solr.repository.SolrCrudRepository;

import java.util.List;

public interface WepPageAnalysisRepository  extends SolrCrudRepository<WebPageAnalysis, String> {

    public List<WebPageAnalysis> findByName(String name);

    @Query("id:*?0* OR pagePath:*?0*")
    public Page<WebPageAnalysis> findByCustomQuery(String searchTerm, Pageable pageable);

  /*  @Query(name = "WebPageAnalysis.findByNamedQuery")
    public Page<WebPageAnalysis> findByNamedQuery(String searchTerm, Pageable pageable);*/

}
