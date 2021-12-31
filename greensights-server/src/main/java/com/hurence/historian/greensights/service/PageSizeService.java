package com.hurence.historian.greensights.service;

import com.google.gson.Gson;
import com.hurence.historian.greensights.repository.WebPageAnalysisRepository;
import com.hurence.historian.greensights.model.solr.WebPageAnalysis;
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
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;
import java.util.logging.Level;

@Service
public class PageSizeService {

    @Autowired
    private WebPageAnalysisRepository webPageAnalysisRepository;

    private static final Logger log = LogManager.getLogger(PageSizeService.class);

    public PageSizeService() {
        WebDriverManager.chromedriver().setup();
    }


    @Cacheable("pagesize")
    public WebPageAnalysis getPageSize(String url) {
        log.info("getting page size : "+ url);

        // get the saved version if it exists
        Optional<WebPageAnalysis> webPageAnalysisFromDB = webPageAnalysisRepository.findById(url);
        if(webPageAnalysisFromDB.isPresent())
            return webPageAnalysisFromDB.get();

        // else we need to make the full analysis through a web driver which can be a little slow
        WebPageAnalysis webPageAnalysis = new WebPageAnalysis();
        webPageAnalysis.setId(url);
        WebDriver driver = setupWebDriver();

        // Your test logic here
        long start = System.currentTimeMillis();
        driver.get(url);
        webPageAnalysis.setDownloadDuration( System.currentTimeMillis() - start);

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
        webPageAnalysis.setPageSizeInBytes(totalDataReceived);
        webPageAnalysis.setNumRequests(numRequests);
        driver.quit();


        return webPageAnalysisRepository.save(webPageAnalysis);
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





}
