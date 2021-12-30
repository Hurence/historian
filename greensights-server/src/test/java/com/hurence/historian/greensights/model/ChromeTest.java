package com.hurence.historian.greensights.model;

import com.hurence.historian.greensights.service.PageSizeService;
import org.junit.jupiter.api.*;
import org.openqa.selenium.WebDriver;

import io.github.bonigarcia.wdm.WebDriverManager;

class ChromeTest {

    WebDriver driver;

    @BeforeAll
    static void setupClass() {
        WebDriverManager.chromedriver().setup();
    }

    @BeforeEach
    void setupTest() {
        PageSizeService pageSizeService = new PageSizeService();
        driver = pageSizeService.setupWebDriver();
    }

    @AfterEach
    void teardown() {
        if (driver != null) {
            driver.quit();
        }
    }




    @Test
    void test() {
        // Your test logic here
        driver.get("https://hurence.com");
        Assertions.assertEquals( "Hurence – The official Hurence website", driver.getTitle());
    }

}