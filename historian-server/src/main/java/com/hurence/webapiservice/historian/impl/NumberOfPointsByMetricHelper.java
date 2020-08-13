package com.hurence.webapiservice.historian.impl;

import java.util.List;

public interface NumberOfPointsByMetricHelper {

    StringBuilder getExpression(StringBuilder exprBuilder, List<String> neededFields);

    String getStreamExpression();
}
