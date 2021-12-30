package com.hurence.historian.greensights.controller;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.WebAppCrawlingSettings;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController()
public class GreensightsController {

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
}
