package com.hurence.webapiservice.http.api.grafana.modele;

public class SearchRequestParam {

    public static final String DEFAULT_STRING_TO_USE_TO_FIND_METRICS = "";
    public static final int DEFAULT_MAX_NUMBER_OF_METRIC_TO_RETURN = 100;

    private String stringToUseToFindMetrics;
    private int maxNumberOfMetricNameToReturn;

    private SearchRequestParam() { }

    public String getStringToUseToFindMetrics() {
        return stringToUseToFindMetrics;
    }

    private void setStringToUseToFindMetrics(String stringToUseToFindMetrics) {
        this.stringToUseToFindMetrics = stringToUseToFindMetrics;
    }

    public int getMaxNumberOfMetricNameToReturn() {
        return maxNumberOfMetricNameToReturn;
    }

    private void setMaxNumberOfMetricNameToReturn(int maxNumberOfMetricNameToReturn) {
        this.maxNumberOfMetricNameToReturn = maxNumberOfMetricNameToReturn;
    }

    public static final class Builder {
        private String stringToUseToFindMetrics;
        private int maxNumberOfMetricNameToReturn;

        public Builder() { }

        public Builder withStringToUSeToFindMetrics(String stringToUseToFindMetrics) {
            this.stringToUseToFindMetrics = stringToUseToFindMetrics;
            return this;
        }

        public Builder withMaxNumberOfMetricToReturn(int maxNumberOfMetricNameToReturn) {
            this.maxNumberOfMetricNameToReturn = maxNumberOfMetricNameToReturn;
            return this;
        }

        public SearchRequestParam build() {
            SearchRequestParam getTimeSerieRequestParam = new SearchRequestParam();
            getTimeSerieRequestParam.setStringToUseToFindMetrics(stringToUseToFindMetrics);
            getTimeSerieRequestParam.setMaxNumberOfMetricNameToReturn(maxNumberOfMetricNameToReturn);
            return getTimeSerieRequestParam;
        }
    }
}
