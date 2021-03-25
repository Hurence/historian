package com.hurence.webapiservice.http.api.grafana.model;

public class SearchValuesRequestParam {
    public static final String DEFAULT_STRING_TO_USE_TO_FIND_METRICS = "";
    public static final int DEFAULT_MAX_NUMBER_OF_METRIC_TO_RETURN = 100;

    private String fieldToSearch;
    private String queryToUseInSearch;
    private int maxNumberOfMetricNameToReturn;

    private SearchValuesRequestParam() { }

    public String getFieldToSearch() {
        return fieldToSearch;
    }

    public String getQueryToUseInSearch() {
        return queryToUseInSearch;
    }

    public int getMaxNumberOfMetricNameToReturn() {
        return maxNumberOfMetricNameToReturn;
    }

    public void setFieldToSearch(String fieldToSearch) {
        this.fieldToSearch = fieldToSearch;
    }

    public void setQueryToUseInSearch(String queryToUseInSearch) {
        this.queryToUseInSearch = queryToUseInSearch;
    }

    public void setMaxNumberOfMetricNameToReturn(int maxNumberOfMetricNameToReturn) {
        this.maxNumberOfMetricNameToReturn = maxNumberOfMetricNameToReturn;
    }

    public static final class Builder {
        private String fieldToSearch;
        private String queryToUseInSearch;
        private int maxNumberOfMetricNameToReturn;

        public Builder() { }

        public SearchValuesRequestParam.Builder withField(String field) {
            this.fieldToSearch = field;
            return this;
        }

        public SearchValuesRequestParam.Builder withQuery(String query) {
            this.queryToUseInSearch = query;
            return this;
        }

        public SearchValuesRequestParam.Builder withMaxNumberOfMetricToReturn(int maxNumberOfMetricNameToReturn) {
            this.maxNumberOfMetricNameToReturn = maxNumberOfMetricNameToReturn;
            return this;
        }

        public SearchValuesRequestParam build() {
            SearchValuesRequestParam getTimeSerieRequestParam = new SearchValuesRequestParam();
            getTimeSerieRequestParam.setFieldToSearch(fieldToSearch);
            getTimeSerieRequestParam.setQueryToUseInSearch(queryToUseInSearch);
            getTimeSerieRequestParam.setMaxNumberOfMetricNameToReturn(maxNumberOfMetricNameToReturn);
            return getTimeSerieRequestParam;
        }
    }
}
