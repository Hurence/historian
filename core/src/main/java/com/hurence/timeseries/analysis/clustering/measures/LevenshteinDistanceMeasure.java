package com.hurence.timeseries.analysis.clustering.measures;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.ml.distance.DistanceMeasure;

import java.util.Arrays;

/**
 * In information theory and computer science, the Levenshtein distance is a
 * string metric for measuring the difference between two sequences.
 * <p>
 * Informally, the Levenshtein distance between two words is the minimum number
 * of single-character edits (insertion, deletion, substitution) required to
 * change one word into the other. The phrase edit distance is often used to
 * refer specifically to Levenshtein distance.
 * <p>
 * It is named after Vladimir Levenshtein, who considered this distance in 1965.
 * [1] It is closely related to pairwise string alignments.
 *
 * @author tom
 */
public class LevenshteinDistanceMeasure implements DistanceMeasure {

    @Override
    public double compute(double[] a, double[] b) throws DimensionMismatchException {

        if (a.length != b.length)
            throw new DimensionMismatchException(a.length, b.length);

        StringBuilder bufferA = new StringBuilder();
        StringBuilder bufferB = new StringBuilder();

        for (int i=0 ; i< a.length; i++) {
            bufferA.append(Character.valueOf((char) a[i]));
            bufferB.append(Character.valueOf((char) b[i]));
        }

        return computeNormalized(bufferA.toString(), bufferB.toString());
    }

    public int costOfSubstitution(char a, char b) {
        return a == b ? 0 : 1;
    }

    public int min(int... numbers) {
        return Arrays.stream(numbers)
                .min().orElse(Integer.MAX_VALUE);
    }

    public int compute(String x, String y) {
        int[][] dp = new int[x.length() + 1][y.length() + 1];

        for (int i = 0; i <= x.length(); i++) {
            for (int j = 0; j <= y.length(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = min(dp[i - 1][j - 1]
                                    + costOfSubstitution(x.charAt(i - 1), y.charAt(j - 1)),
                            dp[i - 1][j] + 1,
                            dp[i][j - 1] + 1);
                }
            }
        }

        return dp[x.length()][y.length()];
    }

    public double computeNormalized(String x, String y) {
        double max = (double) Math.max(x.length(), y.length());

        if (max != 0.0) {
            return (double) compute(x, y) / max;
        } else
            return 0.0;
    }
/*
    private static int minimum(int a, int b, int c) {
        return Math.min(Math.min(a, b), c);
    }

    public static int distance(CharSequence str1,
                               CharSequence str2) {
        int[][] distance = new int[str1.length() + 1][str2.length() + 1];

        for (int i = 0; i <= str1.length(); i++) {
            distance[i][0] = i;
        }
        for (int j = 1; j <= str2.length(); j++) {
            distance[0][j] = j;
        }

        for (int i = 1; i <= str1.length(); i++) {
            for (int j = 1; j <= str2.length(); j++) {
                distance[i][j] = minimum(
                        distance[i - 1][j] + 1,
                        distance[i][j - 1] + 1,
                        distance[i - 1][j - 1]
                                + ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0
                                : 1));
            }
        }

        return distance[str1.length()][str2.length()];
    }

    public static double normalizedDistance(CharSequence str1,
                                            CharSequence str2) {
        double max = (double) Math.max(str1.length(), str2.length());

        if (max != 0.0) {
            return (double) distance(str1, str2) / max;
        }else
            return 0.0;
    }*/
}
