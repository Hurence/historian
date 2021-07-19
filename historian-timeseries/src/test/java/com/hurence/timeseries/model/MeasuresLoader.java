package com.hurence.timeseries.model;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

public class MeasuresLoader {


    public static List<Measure> loadFromCSVFile(String csvFilePath){
        InputStream resource = MeasuresLoader.class.getResourceAsStream(csvFilePath);


        // TODO : load csv file with jackson
        // https://github.com/FasterXML/jackson-dataformats-text/tree/master/csv

        return Collections.emptyList();

    }
}
