package com.hurence.logisland.timeseries.sax;

        import java.util.*;
        import java.util.stream.Collectors;

        import static java.util.stream.Collectors.*;
/**
 * Class: anomaly detection in sax coding strings
 * @author Mejdeddine Nemsi
 * @version 1.0
 *Example:
 * input string "dededdecccdcedeedccddcddccedeeeeeedceefedbdbccdcccp"
 * calculate the array of distances between characters [1, 1, 1, 1, 0, 1, 2, 0, 0, 1, 1, 2, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 8, 6, 1, 1, 0, 0, 0, 0, 0, 1, 1, 2, 0, 1, 1, 1, 2, 2, 2, 1, 0, 1, 1, 0, 0, 13]
 * count occurrences (0 -> 17, 1 -> 25, 6 -> 1, 13 -> 1, 2 -> 6, 8 -> 1)
 * normalise occurrences number (0 -> 0.3333333333333333, 1 -> 0.49019607843137253, 6 -> 0.0196078431372549, 13 -> 0.0196078431372549, 2 -> 0.11764705882352941, 8 -> 0.0196078431372549)
 * filter to less then 10% of total occurrences (6 -> 0.0196078431372549, 13 -> 0.0196078431372549)
 * saxThreshold function returns list of last map keys : [6,13]
 * we use these distances in anomalyDetect function which will return the positions of anomalies [25,26,50]
 */
public class SaxAnalyzer {
    /**
     *Method that counts the number of occurrence of intergers in an array
     * @param numbersToProcess array of integer
     * @return map of each element of input array and it's occurrences
     */
    public static Map<Integer, Integer> countOccurrence(int[] numbersToProcess) {
        final int alphabeticMax = 30;
        int[] possibleNumbers = new int[alphabeticMax];
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();

        for (int i = 0; i < numbersToProcess.length; ++i) {
            possibleNumbers[numbersToProcess[i]] = possibleNumbers[numbersToProcess[i]] + 1;
            result.put(numbersToProcess[i], possibleNumbers[numbersToProcess[i]]);
        }
        return result;
    }

    /**
     * Method that converts a string into an array of characters
     * @param str input sax coding string
     * @return array of characters
     */
    public static char[] convertStringToCharArray (String str){
        char[] result = new char[str.length()];
        for (int i = 0; i < str.length(); i++)
            result[i] = str.charAt(i);
        return result;
    }

    /**
     * Method that counts the distance between characters in an array of characters
     * @param strArray array of characters
     * @return array of integers, empty list in case of empty input
     */
    public static int[] charDistanceAbs (char[] strArray){
        if (strArray.length == 0){
            int reslut[] = {};
            return reslut;}
        else{
            int[] result1 = new int[strArray.length-1];
            for (int i = 0; i < strArray.length-1; i++)
                result1[i] = Math.abs(strArray [i+1] - strArray[i]);
            return result1;}
    }

    /**
     * Method that returns the search for abnormal distances between characters.
     * @param str string (sax coding string)
     * @return list of abnormal distances between characters
     */
    public static List<Integer> saxThreshold(String str, double threshold){
        //final double threshold = 0.1;
        //Transform our string into an array of characters then into an array of distances between characters
        int[] buff = charDistanceAbs(convertStringToCharArray(str));
        //Counting occurrences
        Map<Integer, Integer> buffCount = countOccurrence(buff);
        //Normalize the occurrences
        Map<Integer, Double> buffCountNorm = buffCount.entrySet().stream().collect(toMap(e -> e.getKey(),
                e -> e.getValue()/((double)(str.length()-1))));
        //filter to less the 10% of total occurrences
        Map<Integer, Double> buffCountNormPer  = (buffCountNorm.entrySet().stream()
                .filter(map -> map.getValue() <threshold)
                .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue())));
        //convert the last map created into a list to be outputed
        List<Integer> finalResult = new ArrayList(buffCountNormPer.keySet());
        if (buffCountNormPer.isEmpty() == true)
            return Collections.emptyList();
        else
            return finalResult;
    }

    /**
     * Method that prints the anomalies positions in our sax coding string
     * @param str string (sax coding string)
     * @param thresh list of anomaly distances between characters in input string
     * @return list of postions of anomalies in sax coding string
     */
    public static List<Integer> anomalyDetect (String str, List<Integer> thresh){

        Boolean res = false;
        List<Integer> finalResult = new ArrayList<>();
        //Transform our string into an array of characters
        char[] strArray = convertStringToCharArray(str);
        //Search for Anomalies
        for (int j = 0; j< str.length()-1; j++)
            if(thresh.contains(Math.abs(strArray [j+1] - strArray[j]))) {
                res = true;
                finalResult.add(j);
            }
        //In the case of no anomaly is detected
        if (res == false)
            finalResult.add(-1);

        return finalResult;
    }
}
