package com.hurence.historian.greensights.controller;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.historian.greensights.model.WebAppCrawlingSettings;
import com.hurence.historian.greensights.model.request.ComputeRequest;
import com.hurence.historian.greensights.service.EnergyImpactComputationService;
import com.hurence.historian.greensights.util.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RestController()
public class GreensightsController {

    @Autowired
    private EnergyImpactComputationService energyImpactComputationService;

    @PostMapping("/crawler")
    WebAppCrawlingSettings newCrawler(@RequestBody WebAppCrawlingSettings webAppCrawlingSettings) {

        return webAppCrawlingSettings;
       // return repository.save(newEmployee);
    }

    @GetMapping("/crawler/{id}/metrics")
    List<EnergyImpactMetric> newCrawler(@PathVariable String crazwlerId) {

        return Collections.emptyList();
        // return repository.save(newEmployee);
    }



    @PostMapping("/compute")
    List<EnergyImpactReport> compute(@RequestBody ComputeRequest computeRequest) {


        if(computeRequest.getDoComputeDayByDay()){
            List<ComputeRequest> requests = DateUtils.requestsBetweenDays(computeRequest);

            return requests.stream()
                    .map(request -> energyImpactComputationService.compute(request))
                    .collect(Collectors.toList());
        }else {
            return Collections.singletonList(energyImpactComputationService.compute(computeRequest));
        }

    }
}
