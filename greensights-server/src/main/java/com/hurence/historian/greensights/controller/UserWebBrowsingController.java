package com.hurence.historian.greensights.controller;

import com.hurence.historian.greensights.model.UserLastHourWebBrowsingMetric;
import com.hurence.historian.greensights.service.UserWebBrowsingService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
@RequestMapping(
        value = "/api/v1/user",
        produces = "application/vnd.hurence.api.v1+json")
@RequiredArgsConstructor
public class UserWebBrowsingController {

    private final UserWebBrowsingService userWebBrowsingService;

    @PostMapping("/web-browsing")
    public UserLastHourWebBrowsingMetric save(@RequestBody UserLastHourWebBrowsingMetric metric) {
            return userWebBrowsingService.save(metric);
    }
}
