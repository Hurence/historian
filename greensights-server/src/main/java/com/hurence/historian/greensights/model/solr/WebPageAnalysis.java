package com.hurence.historian.greensights.model.solr;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.solr.core.mapping.Indexed;
import org.springframework.data.solr.core.mapping.SolrDocument;

import java.util.Date;

@Data
@SolrDocument(collection = "greensights")
@Accessors(fluent = true)
public class WebPageAnalysis {

    @Id
    @Indexed(name = "id", type = "string")
    private String url;

    @Indexed(name = "doc_type_s", type = "string" )
    private String doc_type = "webpage_analysis";

    // the size of the page and of the downloaded elements of the page in bytes
    @Indexed(name = "page_size_in_bytes_l", type = "long" )
    private long pageSizeInBytes;

    // the number of the DOM elements in the page
    @Indexed(name = "page_nodes_i", type = "int" )
    private int pageNodes;

    // the OpenGraph type of the page
    @Indexed(name = "page_type_s", type = "string" )
    private String pageType;

    // the number of external requests made by the page
    @Indexed(name = "num_requests_i", type = "integer")
    private int numRequests;

    // the corresponding ecoindex grade of the page (from A to G)
    @Indexed(name = "ecoindex_grade_s", type = "string")
    private String ecoIndexGrade;

    // the corresponding ecoindex score of the page (0 to 100)
    @Indexed(name = "ecoindex_score_f", type = "float")
    private float ecoIndexScore;

    // the equivalent of greenhouse gases emission (in `gCO2e`) of the page
    @Indexed(name = "ecoindex_ges_f", type = "float")
    private float ecoIndexGES;

    // the equivalent water consumption (in `cl`) of the page
    @Indexed(name = "ecoindex_water_f", type = "float")
    private float ecoIndexWater;

    @Indexed(name = "download_duration_l", type = "long")
    private long downloadDuration;

    @Indexed(name="computation_date_dt")
    private Date computationDate = new Date();

    @Indexed(name="grade_s")
    private String grade;


}
