package com.hurence.historian.greensights.service;

import com.hurence.historian.greensights.model.solr.WebPageAnalysis;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EcoIndexServiceTest {


    @Test
    void testQuantiles() {

        EcoIndexService service = new EcoIndexService();
        Assertions.assertEquals(15.086372025739513, service.getQuantile(EcoIndexService.quantilesSize, 2500));
        Assertions.assertEquals(3.892857142857143, service.getQuantile(EcoIndexService.quantilesDom, 150));
        Assertions.assertEquals(3.8, service.getQuantile(EcoIndexService.quantilesReq, 23));
        Assertions.assertEquals(15.086372025739513, service.getQuantile(EcoIndexService.quantilesSize, 2500));
        Assertions.assertEquals(15.086372025739513, service.getQuantile(EcoIndexService.quantilesSize, 2500));
    }

    @Test
    void testGrade() {
        EcoIndexService service = new EcoIndexService();
        Assertions.assertEquals("G", service.getGrade(2));
        Assertions.assertEquals("F", service.getGrade(10));
        Assertions.assertEquals("E", service.getGrade(25));
        Assertions.assertEquals("F", service.getGrade(10));
        Assertions.assertEquals("C", service.getGrade(50.2f));
        Assertions.assertEquals("F", service.getGrade(10));
        Assertions.assertEquals("A", service.getGrade(100));
    }


    @Test
    void testWaterConsumption() {
        EcoIndexService service = new EcoIndexService();
        Assertions.assertEquals(4.440000057220459, service.getWaterConsumption(2));
        Assertions.assertEquals(4.199999809265137, service.getWaterConsumption(10));
        Assertions.assertEquals(3, service.getWaterConsumption(50));
        Assertions.assertEquals(2.4000000953674316, service.getWaterConsumption(70));
    }


    @Test
    void testGreenhouseGasesEmission() {
        EcoIndexService service = new EcoIndexService();

        Assertions.assertEquals(2.9600000381469727, service.getGreenhouseGasesEmission(2));
        Assertions.assertEquals(2.799999952316284, service.getGreenhouseGasesEmission(10));
        Assertions.assertEquals(2, service.getGreenhouseGasesEmission(50));
        Assertions.assertEquals(1.600000023841858, service.getGreenhouseGasesEmission(70));
    }


    @Test
    void testScore() {
        EcoIndexService service = new EcoIndexService();
        Assertions.assertEquals(67, service.getScore(new WebPageAnalysis().pageNodes(100).numRequests(100).pageSizeInBytes(100 * 100)));
        Assertions.assertEquals(62, service.getScore(new WebPageAnalysis().pageNodes(100).numRequests(100).pageSizeInBytes(1000 * 100)));
        Assertions.assertEquals(53, service.getScore(new WebPageAnalysis().pageNodes(100).numRequests(100).pageSizeInBytes(10000 * 100)));
        Assertions.assertEquals(41, service.getScore(new WebPageAnalysis().pageNodes(200).numRequests(200).pageSizeInBytes(10000 * 100)));
        Assertions.assertEquals(5, service.getScore(new WebPageAnalysis().pageNodes(2355).numRequests(267).pageSizeInBytes(2493 * 100)));
        Assertions.assertEquals(78, service.getScore(new WebPageAnalysis().pageNodes(240).numRequests(20).pageSizeInBytes(331 * 100)));
    }


}
