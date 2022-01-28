package com.hurence.historian.greensights.service;


import com.hurence.historian.greensights.model.solr.WebPageAnalysis;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.round;

/**
 * https://github.com/cnumr/ecoindex_python/blob/master/ecoindex/ecoindex.py
 */
@Service
public class EcoIndexService {

    static List<Double> quantilesDom = Arrays.asList(0.0, 47.0, 75.0, 159.0, 233.0, 298.0, 358.0, 417.0, 476.0, 537.0, 603.0, 674.0, 753.0, 843.0, 949.0, 1076.0, 1237.0, 1459.0, 1801.0, 2479.0, 594601.0);
    static List<Double> quantilesReq = Arrays.asList(0.0, 2.0, 15.0, 25.0, 34.0, 42.0, 49.0, 56.0, 63.0, 70.0, 78.0, 86.0, 95.0, 105.0, 117.0, 130.0, 147.0, 170.0, 205.0, 281.0, 3920.0);
    static List<Double> quantilesSize = Arrays.asList(0.0, 1.37, 144.7, 319.53, 479.46, 631.97, 783.38, 937.91, 1098.62, 1265.47, 1448.32, 1648.27, 1876.08, 2142.06, 2465.37, 2866.31, 3401.59, 4155.73, 5400.08, 8037.54, 223212.26);


    double getQuantile(List<Double> quantiles, int value) {
        for (int i = 1; i < quantiles.size() + 1; i++) {
            if (value < quantiles.get(i))
                return i + (value - quantiles.get(i - 1)) / (quantiles.get(i) - quantiles.get(i - 1));
        }

        return quantiles.size();
    }


    String getGrade(float ecoindex) {
        if (ecoindex > 75)
            return "A";
        else if (ecoindex > 65)
            return "B";
        else if (ecoindex > 50)
            return "C";
        else if (ecoindex > 35)
            return "D";
        else if (ecoindex > 20)
            return "E";
        else if (ecoindex > 5)
            return "F";
        return "G";
    }

    float getGreenhouseGasesEmission(float ecoindex) {
        return round(100 * (2 + 2 * (50 - ecoindex) / 100.0f)) / 100.0f;
    }

    float getWaterConsumption(float ecoindex) {
        return round(100 * (3 + 3 * (50 - ecoindex) / 100.0f)) / 100.0f;
    }

    public WebPageAnalysis computeEcoIndex(WebPageAnalysis webPageAnalysis) {
        float ecoIndexScore = getScore(webPageAnalysis);

        webPageAnalysis.ecoIndexScore(ecoIndexScore);
        webPageAnalysis.ecoIndexGES(getGreenhouseGasesEmission(ecoIndexScore));
        webPageAnalysis.ecoIndexWater(getWaterConsumption(ecoIndexScore));
        webPageAnalysis.ecoIndexGrade(getGrade(ecoIndexScore));
        return webPageAnalysis;
    }

    float getScore(WebPageAnalysis webPageAnalysis) {
        double qDom = getQuantile(quantilesDom, webPageAnalysis.pageNodes());
        double qSize = getQuantile(quantilesSize, (int) (webPageAnalysis.pageSizeInBytes() / 100.0f));
        double qReq = getQuantile(quantilesReq, webPageAnalysis.numRequests());

        float ecoIndexScore = round( (100 - 5 * (3 * qDom + 2 * qReq + qSize) / 6.0f));
        return ecoIndexScore;
    }

}
