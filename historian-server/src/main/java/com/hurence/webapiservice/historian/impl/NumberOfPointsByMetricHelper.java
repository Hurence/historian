package com.hurence.webapiservice.historian.impl;

import java.util.List;

public interface NumberOfPointsByMetricHelper {

    //TODO this should be hidden, no need to be made public (so should not be present in the interface)
    StringBuilder getExpression(StringBuilder exprBuilder, List<String> neededFields);

    String getStreamExpression();
}
