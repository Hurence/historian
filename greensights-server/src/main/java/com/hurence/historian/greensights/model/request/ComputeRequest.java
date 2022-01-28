package com.hurence.historian.greensights.model.request;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class ComputeRequest {

    private String startDate;

    private String endDate;

    private Boolean doSaveMeasures = false;

    private Boolean doSaveMetrics = true;

    private Boolean doComputeDayByDay = false;

    private List<String> rootUrlFilters = new ArrayList<>();

    private List<String> accountFilters = new ArrayList<>();
}
