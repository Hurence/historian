package com.hurence.timeseries.model;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MeasuresLoader {


    public static List<Measure> loadFromCSVFile(String csvFilePath) throws IOException {
        InputStream resource = MeasuresLoader.class.getResourceAsStream(csvFilePath);


        // TODO : load csv file with jackson
        // https://github.com/FasterXML/jackson-dataformats-text/tree/master/csv

        File csvFile = new File(csvFilePath).getAbsoluteFile();

        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        MappingIterator<List<String>> rows = csvMapper.readerFor(List.class).readValues(csvFile);
        ArrayList<Measure> loadedData = new ArrayList<Measure>();

        for (MappingIterator<List<String>> it = rows; it.hasNext(); ) {
            List<String> row = it.next();
            if (!row.get(0).equals("timestamp")) {
                loadedData.add(Measure.fromValue(Long.parseLong(row.get(0)), Double.parseDouble(row.get(1))));
            }
        }
        return loadedData;
    }
}
