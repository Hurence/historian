package com.hurence.webapiservice.historian.impl;

public interface NumberOfPointsByMetricHelper {

    /**
     *  used to get stream expression to use depending on if quality matters or not.
     *
     * @return StringBuilder to be used in the solr Stream query
     */
    String getStreamExpression();
}
