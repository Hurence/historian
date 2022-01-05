package com.hurence.historian.greensights.model.request;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ComputeRequest {

    private String startDate;

    private String endDate;

    private Boolean doSaveMeasures = false;

    private Boolean doSaveMetrics = true;

    private Boolean doComputeDayByDay = false;
}
