package com.hurence.historian.greensights.service;

import com.google.gson.Gson;
import com.hurence.historian.greensights.model.EnergyImpactMetric;
import io.github.bonigarcia.wdm.WebDriverManager;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.logging.LogEntries;
import org.openqa.selenium.logging.LogEntry;
import org.openqa.selenium.logging.LogType;
import org.openqa.selenium.logging.LoggingPreferences;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.logging.Level;

import static org.apache.logging.log4j.ThreadContext.containsKey;

@Service
public class PageSizeService {

    public PageSizeService() {
        WebDriverManager.chromedriver().setup();
    }



    @Cacheable("pagesize")
    public long getPageSize(String url) {
        log.info("getting page size : "+ url);
        return getPageSizeData(url);
    }

    private long getPageSizeData(String url){
        WebDriver driver = setupWebDriver();


        // Your test logic here
        driver.get(url);

        LogEntries logEntries = driver.manage().logs().get(LogType.PERFORMANCE);
        long totalDataReceived = 0L;
        int numRequests = 0;
        for (LogEntry entry : logEntries) {
            Gson gson = new Gson();
            LogData logData = gson.fromJson(entry.getMessage(), LogData.class);

            if (logData.getMessage().getMethod().equals("Network.loadingFinished")) {
                totalDataReceived += logData.getMessage().getParams().getEncodedDataLength();
                numRequests++;
            }
        }

        driver.quit();
        return totalDataReceived;
    }

    @Data
    class LogData {
        private LogMessage message;
    }

    @Data
    class LogMessage {
        private String method;
        private LogParams params;
    }

    @Data
    class LogParams {
        private int dataLength;
        private int encodedDataLength;
        private String requestId;
        private LogResponse response;
    }

    @Data
    class LogResponse {
        private boolean fromDiskCache;
        private int encodedDataLength;
    }


    public WebDriver setupWebDriver() {



        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless");
        options.addArguments("--disable-gpu");
        options.addArguments("--window-size=1400,800");
        options.addArguments("--whitelisted-ips=");


        LoggingPreferences logPrefs = new LoggingPreferences();
        logPrefs.enable(LogType.BROWSER, Level.ALL);
        logPrefs.enable(LogType.PERFORMANCE, Level.ALL);
        options.setCapability("goog:loggingPrefs", logPrefs);
        return new ChromeDriver(options);
    }

    private static final Logger log = LogManager.getLogger(PageSizeService.class);

    public List<EnergyImpactMetric> updateMetrics(List<EnergyImpactMetric> energyImpactMetrics) {


        energyImpactMetrics.forEach(energyImpactMetric -> {
            WebDriver driver = setupWebDriver();


            // Your test logic here
            driver.get(energyImpactMetric.getFullUrl());

            LogEntries logEntries = driver.manage().logs().get(LogType.PERFORMANCE);
            long totalDataReceived = 0L;
            int numRequests = 0;
            for (LogEntry entry : logEntries) {
                Gson gson = new Gson();
                LogData logData = gson.fromJson(entry.getMessage(), LogData.class);

                if (logData.getMessage().getMethod().equals("Network.loadingFinished")) {
                    totalDataReceived += logData.getMessage().getParams().getEncodedDataLength();
                    numRequests++;
                }
            }
            energyImpactMetric.setPageSizeInBytes(totalDataReceived);
            driver.quit();
        });


        return energyImpactMetrics;
    }


}
