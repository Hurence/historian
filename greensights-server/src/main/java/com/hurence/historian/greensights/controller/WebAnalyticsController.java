package com.hurence.historian.greensights.controller;

import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.historian.greensights.model.request.ComputeRequest;
import com.hurence.historian.greensights.service.EnergyImpactComputationService;
import com.hurence.historian.greensights.util.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController()
@RequestMapping(
        value = "/api/v1/web-analytics",
        produces = "application/vnd.hurence.api.v1+json")
public class WebAnalyticsController {

    @Autowired
    private EnergyImpactComputationService energyImpactComputationService;



    @PostMapping("/compute")
    List<EnergyImpactReport> compute(@RequestBody ComputeRequest computeRequest) {


        if(computeRequest.getDoComputeDayByDay()){
            List<ComputeRequest> requests = DateUtils.requestsBetweenDays(computeRequest);

            return requests.stream()
                    .flatMap(request -> energyImpactComputationService.compute(request).stream())
                    .collect(Collectors.toList());
        }else {
            return energyImpactComputationService.compute(computeRequest);
        }

    }
}
