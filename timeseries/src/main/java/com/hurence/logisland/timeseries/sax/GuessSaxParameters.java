package com.hurence.logisland.timeseries.sax;

import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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
            *          input data {5.635,5.635,5.64,5.64,5.64,5.65,5.66,5.68,5.7,5.71,5.71,5.715,5.72,5.73,5.735,5.74,
    * 5.77,5.78,5.775,5.78,5.79,5.785,5.79,5.8,5.81,5.815,5.8,5.805,5.815,5.82,5.82,5.82,
    * 5.825,5.82,5.83,5.83,5.82,5.81,5.815,5.82,5.82,5.815,5.815,5.8,5.79,5.78,5.77}
    *
            *          calculate rulepruner parameters for multiple sax windows, PAA and Alphabets
    *          sort our clusters of parameters through the use of "reduction" of each cluster of parameters
    *          The optimal choice of parameters is where the "reduction" is the lowest
    *          the guess method will return a list of the optimal window size, PAA size, alphabet size, Approx Distance,
    *          Grammar size, Grammar rules, compressed grammar size, prune rules, coverage, reduction et max frequency:
            *          [12, 4, 9, 1.4450012717343763, 100, 6, 100, 84, 2, 0.5531914893617021, 100, 0.3333333333333333, 2]
     *               * @param points
     *      * @return
     *      */
    public List<Float> computeBestParam(List<Double> points)  {

        double[] valuePoints = points.stream().mapToDouble(x -> x).toArray();



            return null;


    }


    public static final class Builder {

        private int paaSize = 3;
        private double nThreshold = 0;
        private int alphabetSize = 3;

        public GuessSaxParameters.Builder paaSize(final int paaSize) {
            this.paaSize = paaSize;
            return this;
        }
        public GuessSaxParameters.Builder nThreshold(final double nThreshold) {
            this.nThreshold = nThreshold;
            return this;
        }
        public GuessSaxParameters.Builder alphabetSize(final int alphabetSize) {
            this.alphabetSize = alphabetSize;
            return this;
        }

        /**
         * @return a BinaryCompactionConverter as configured
         *
         */
        public GuessSaxParameters build() {
            return new GuessSaxParameters(new SAXOptionsImpl(paaSize, nThreshold, alphabetSize));
        }
    }

}
