package com.hurence.logisland.timeseries.sax;

import com.sun.javafx.collections.MappingChange;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;


public class SaxAnalyzer {

    public static Map<Integer, Integer> countOccurrence(int[] numbersToProcess) {
        int[] possibleNumbers = new int[30];
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();

        for (int i = 0; i < numbersToProcess.length; ++i) {
            possibleNumbers[numbersToProcess[i]] = possibleNumbers[numbersToProcess[i]] + 1;
            result.put(numbersToProcess[i], possibleNumbers[numbersToProcess[i]]);
        }

        return result;
    }
    public static List<Integer> saxThreshold(String str){
        double l = str.length();
        //Transform our string into an array of characters
        char[] strArray = new char[str.length()];
        for (int i = 0; i < str.length(); i++)
            strArray[i] = str.charAt(i);
        //Array of distances between characters
        int[] buff = new int[str.length()-1];
        for (int j =0; j< str.length()-1; j++)
            buff[j] = Math.abs((int)(strArray [j+1] - strArray[j]));

        //Counting occurrences
        Map<Integer, Integer> buffCount = countOccurrence( buff);
        //Normalize the occurrences
        Map<Integer, Double> buffCountNorm = buffCount.entrySet().stream().collect(toMap(e -> e.getKey(),
                e -> e.getValue()/(l-1)));
        //filter to less the 10% of total occurrences
        Map<Integer, Double> buffCountNormPer  = (buffCountNorm.entrySet().stream()
                .filter(map -> map.getValue() <0.1)
                .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue())));
        //get the threshold : min of buffCountNormP keys
        List<Integer> finalResult = new ArrayList(buffCountNormPer.keySet());
        if (buffCountNormPer.isEmpty() == true)
            return Collections.emptyList();
        else
            return finalResult;
    }

    public static List<Integer> anomalyDetect (String str, List<Integer> thresh){
        //Method that prints the anomalies position in our string of data
        Boolean res = false;
        List<Integer> finalResult = new ArrayList<>();
        //Transform our string into an array of characters
        char[] strArray = new char[str.length()];
        for (int i = 0; i < str.length(); i++)
            strArray[i] = str.charAt(i);
        //Search for Anomalies
        for (int j = 0; j< str.length()-1; j++)
            if( thresh.contains(Math.abs((int)(strArray [j+1] - strArray[j])))) {
                res = true;
                finalResult.add(j);
            }
        //In the case of no anomaly is detected
        if (res == false) {finalResult.add(-1);}


        return finalResult;


    }



    public static void main(String []args){
        String str = "saxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxh";
        System.out.println(anomalyDetect(str , saxThreshold(str)));
    }
}
