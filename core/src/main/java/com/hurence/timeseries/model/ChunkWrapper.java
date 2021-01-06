package com.hurence.timeseries.model;

import com.hurence.timeseries.analysis.clustering.ChunkClusterable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ChunkWrapper implements ChunkClusterable {

    private String sax;
    private String id;
    private Map<String, String> tags = new HashMap<>();

    public ChunkWrapper(String id, String sax, Map<String, String> tags) {
        this.sax = sax;
        this.id = id;
        this.tags = tags;
    }

    @Override
    public String getSax() {
        return sax;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

  /*  @Override
    public double[] getPoint() {
        assert sax != null;
        double[] saxAsDoubles = new double[sax.length()];

        for (int i = 0; i < sax.length(); i++) {
            saxAsDoubles[i] = (Character.getNumericValue(sax.charAt(i)) - 10.0);
        }

        return saxAsDoubles;
    }*/

    @Override
    public double[] getPoint() {
        assert sax != null;



        double[] saxAsDoubles = new double[sax.length()+2];

        for (int i = 0; i < sax.length(); i++) {
            saxAsDoubles[2+i] = (Character.getNumericValue(sax.charAt(i)) - 10.0);
        }




        int[] lettersDitrsibution = new int[7];
        double[] dimensions = new double[6];

        char previousChar = sax.charAt(0);
        int numConsecutiveEqualsValues = 0;
        int slopeChangesCount = 0;
        boolean previousPositiveSlope = false;
        boolean positiveSlope = false;
        for (int i = 0; i < sax.length(); i++) {
            char currentChar = sax.charAt(i);

            // stability
            if (previousChar - currentChar == 0) {
                saxAsDoubles[0]++;
                numConsecutiveEqualsValues++;
            }else
                numConsecutiveEqualsValues =0;

            // consecutiveness
            if(numConsecutiveEqualsValues>=2){
                saxAsDoubles[1]++;
            }

            // big steps up
           if (previousChar - currentChar >= 2)
                dimensions[2]++;

            // big steps up
            if (currentChar -previousChar>= 2)
                dimensions[3]++;


            // slope changes
            if (previousChar - currentChar > 0 ) {
                positiveSlope = true;
            }else if (previousChar - currentChar < 0 ) {
                positiveSlope = false;
            }

            if(positiveSlope != previousPositiveSlope) {
                previousPositiveSlope = positiveSlope;
                slopeChangesCount++;
            }

            previousChar = currentChar;


            // letter distrib
            switch (currentChar) {
                case 'a':
                    lettersDitrsibution[0]++;
                    break;
                case 'b':
                    lettersDitrsibution[1]++;
                    break;
                case 'c':
                    lettersDitrsibution[2]++;
                    break;
                case 'd':
                    lettersDitrsibution[3]++;
                    break;
                case 'e':
                    lettersDitrsibution[4]++;
                    break;
                case 'f':
                    lettersDitrsibution[5]++;
                    break;
                case 'g':
                    lettersDitrsibution[6]++;
                    break;
            }
        }

        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (int i = 0; i < 7; i++) {
            stats.addValue (lettersDitrsibution[i] );
        }

        return saxAsDoubles;
    }
}
