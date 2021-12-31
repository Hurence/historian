package com.hurence.historian.greensights.model.solr;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.solr.core.mapping.Indexed;
import org.springframework.data.solr.core.mapping.SolrDocument;

import java.util.Date;

@Data
@SolrDocument(collection = "greensights")
public class WebPageAnalysis {

    @Id
    @Indexed(name = "id", type = "string")
    private String id;

    @Indexed(name = "page_size_in_bytes_l", type = "long" )
    private long pageSizeInBytes;

    @Indexed(name = "num_requests_i", type = "integer")
    private int numRequests;

    @Indexed(name = "download_duration_l", type = "long")
    private long downloadDuration;

    @Indexed(name="last_crawling_date_dt")
    private Date lastCrawlingDate = new Date();
}
