package com.hurence.logisland.timeseries.sax;

import net.seninp.gi.GIAlgorithm;
import net.seninp.gi.rulepruner.ReductionSorter;
import net.seninp.gi.rulepruner.RulePruner;
import net.seninp.gi.rulepruner.RulePrunerParameters;
import net.seninp.gi.rulepruner.SampledPoint;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class GuessSaxParameters {



    private static Logger logger = LoggerFactory.getLogger(SaxConverter.class.getName());
    public static SAXProcessor saxProcessor = new SAXProcessor();
    public static NormalAlphabet normalAlphabet = new NormalAlphabet();

    private SAXOptions saxOptions;

    public class SaxParameters{


    }

    private GuessSaxParameters(SAXOptions saxOptions) {
        this.saxOptions = saxOptions;
    }


    /**
     *


        * @author Mejdeddine Nemsi
    * @version 1.0 *Example:
    *input data {5.635,5.635,5.64,5.64,5.64,5.65,5.66,5.68,5.7,5.71,5.71,5.715,5.72,5.73,5.735,5.74,
    * 5.77,5.78,5.775,5.78,5.79,5.785,5.79,5.8,5.81,5.815,5.8,5.805,5.815,5.82,5.82,5.82,
    * 5.825,5.82,5.83,5.83,5.82,5.81,5.815,5.82,5.82,5.815,5.815,5.8,5.79,5.78,5.77}
    *
    *
    * the guess method will return a list of the optimal window size, PAA size, alphabet size, Approx Distance,
    * Grammar size, Grammar rules, compressed grammar size, prune rules, coverage, reduction and max frequency:
     *[12, 4, 9, 1.4450012717343763, 100, 6, 100, 84, 2, 0.5531914893617021, 100, 0.3333333333333333, 2]
     *
     * Window size: Sliding window size, it's specified at the beginning (from 10 to a doubled length of typical structural phenomenon observed in the time series)
     *              after the calculation of the optimal parameters we get the best sliding window size
     * PAA size: Piecewise Aggregation Approximation (of timeseries) it's used to reduce dimensionality of the input timeseries by splitting the into equal-sized segments
     *            which are computed by averaging the values in these segments.
     *            the typical range can be specified from 2->50 (Window size is always greater then PAA size)
     * Alphabet size: 'a' Alphabet size (e.g., for the alphabet = {a,b,c}, a = 3). It is specified btween 2 and 15
     * Approx Distance: Or we can call it Approximation error. It can be seen as the sum of two error values computed for each subsequence extracted via sliding window
     * Grammar size: the number of suggested alphabet letters
     * Grammar rules: Each repeated substring is replaced by grammatical rule that produces a shorter original string
     * Compressed grammar size: Compressed number of suggested alphabet letters
     * Prune rules: It's designed to eliminate redundant grammar rules that do not contribute to timeseries grammar cover
     * coverage: The ratio of (the number of time series points which are located within any of the subsequence corresponding to the grammar rules) by
     *                                                          (the total number of points in the timeseries)
     * Reduction: It's used for selecting the set of optimal discretization parameters
     *            It's the ratio of (number of rules in the pruned grammar) by
     *                               (number of rules in the full grammar)
     *
     *PS: For each parameter (inputed window size, PAA size and Alphabet size) combination a grammar is inferred and pruned in order to compute the reduction coefficient value.
     *Among all sampled and valid combinations we select one which yields the minimal value of the reduction coefficient as optimal discretization parameter set.
     *
     * RQ: NThreshold (normalization threshold value) can be used in SAX to deal with one special case (normalization to a subsequence which is almost constant)
     * as mentioned in "Experiencing SAX: a novel symbolic representation of time series".It assigns the entire word to the middle-ranged alphabet when
     * the standard deviation of current subsequence is slower than NThreshold
     * @param points
     * @return
     */



    public  static ArrayList computeBestParam(List<Double> points )  {

        // discretization parameters
        int WINDOW_START = 0;
        int WINDOW_END = 100;
        int WINDOW_STEP = 20;
        int PAA_START = 2;
        int PAA_END = 30;
        int PAA_STEP = 1;
        int ALPHABETIC_START = 2;
        int ALPHABETIC_END = 8;
        int ALPHABETIC_STEP = 1;


        double[] valuePoints = points.stream().mapToDouble(x -> x).toArray();

        ArrayList result= new ArrayList<>();
        ArrayList<SampledPoint> res = new ArrayList<>();
        RulePruner rp = new RulePruner(valuePoints);


        for (int WINDOW = WINDOW_START; WINDOW < WINDOW_END; WINDOW += WINDOW_STEP) {

            for (int PAA = PAA_START; PAA < PAA_END; PAA += PAA_STEP) {



                // check for invalid cases
                if (PAA > WINDOW) {
                    continue;
                }

                for (int ALPHABET = ALPHABETIC_START; ALPHABET < ALPHABETIC_END; ALPHABET += ALPHABETIC_STEP) {

                    SampledPoint p = null;

                    try {
                        if (valuePoints.length >= WINDOW_END - WINDOW_START) {
                            p = rp.sample(WINDOW, PAA, ALPHABET, GIAlgorithm.REPAIR,
                                    RulePrunerParameters.SAX_NR_STRATEGY, RulePrunerParameters.SAX_NORM_THRESHOLD);
                        }

                        else return result;

                    }
                    catch (Exception e) {
                        System.err.println("Ooops -- was interrupted");
                    }

                    if (null != p) {
                        res.add(p);
                    }

                }
            }
        }

        //comparison is done through the use of reduction param, best parameters have the lowest reduction
        Collections.sort(res, new ReductionSorter());


        // result.add(res.get(0));
        result.add(res.get(0).getWindow());
        result.add(res.get(0).getPAA());
        result.add(res.get(0).getAlphabet());
        result.add(res.get(0).getApproxDist());
        result.add(res.get(0).getGrammarSize());
        result.add(res.get(0).getGrammarRules());
        result.add(res.get(0).getGrammarSize());
        result.add(res.get(0).getCompressedGrammarSize());
        result.add(res.get(0).getPrunedRules());
        result.add(res.get(0).getCoverage());
        result.add(res.get(0).getGrammarSize());
        result.add(res.get(0).getReduction());
        result.add(res.get(0).getMaxFrequency());


        return result;
    }
   }


