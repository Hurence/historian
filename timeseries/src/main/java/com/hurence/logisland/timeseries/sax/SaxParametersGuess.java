package com.hurence.logisland.timeseries.sax;

import net.seninp.gi.GIAlgorithm;
import net.seninp.gi.rulepruner.ReductionSorter;
import net.seninp.gi.rulepruner.RulePruner;
import net.seninp.gi.rulepruner.RulePrunerParameters;
import net.seninp.gi.rulepruner.SampledPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Collections;



/**
 * Class: anomaly detection in sax coding strings
 * @author Mejdeddine Nemsi
 * @version 1.0
 *Example:
 * input data {5.635,5.635,5.64,5.64,5.64,5.65,5.66,5.68,5.7,5.71,5.71,5.715,5.72,5.73,5.735,5.74,
 *             5.77,5.78,5.775,5.78,5.79,5.785,5.79,5.8,5.81,5.815,5.8,5.805,5.815,5.82,5.82,5.82,
 *             5.825,5.82,5.83,5.83,5.82,5.81,5.815,5.82,5.82,5.815,5.815,5.8,5.79,5.78,5.77}
 *
 * calculate rulepruner parameters for multiple sax windows, PAA and Alphabets
 * sort our clusters of parameters through the use of "reduction" of each cluster of parameters
 * The optimal choice of parameters is where the "reduction" is the lowest
 * the guess method will return a list of the optimal window size, PAA size, alphabet size, Approx Distance,
 * Grammar size, Grammar rules, compressed grammar size, prune rules, coverage, reduction et max frequency:
 * [12, 4, 9, 1.4450012717343763, 100, 6, 100, 84, 2, 0.5531914893617021, 100, 0.3333333333333333, 2]
 */
public class SaxParametersGuess {


    private static Logger LOGGER = LoggerFactory.getLogger(SaxParametersGuess.class);





    public static ArrayList guess(double[] ts, int WINDOW_START, int WINDOW_END, int WINDOW_STEP
                                    ,int PAA_START, int PAA_END,int PAA_STEP
                                     ,int ALPHABETIC_START,int ALPHABETIC_END, int ALPHABETIC_STEP ){




        ArrayList result= new ArrayList<>();
        ArrayList<SampledPoint> res = new ArrayList<>();
        RulePruner rp = new RulePruner(ts);


        for (int WINDOW = WINDOW_START; WINDOW < WINDOW_END; WINDOW += WINDOW_STEP) {

            for (int PAA = PAA_START; PAA < PAA_END; PAA += PAA_STEP) {

                //System.out.print(WINDOW + "  ");

                // check for invalid cases
                if (PAA > WINDOW) {
                    continue;
                }

                for (int ALPHABET = ALPHABETIC_START; ALPHABET < ALPHABETIC_END; ALPHABET += ALPHABETIC_STEP) {

                    SampledPoint p = null;

                    try {
                        if (ts.length >= WINDOW_END - WINDOW_START) {
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
        result.add( res.get(0).getWindow());
        result.add( res.get(0).getPAA());
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
