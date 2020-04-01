package com.hurence.logisland.timeseries.timeseries
import spock.lang.Specification
import spock.lang.Unroll

import static com.hurence.logisland.timeseries.sax.SaxAnalyzer.anomalyDetect
import static com.hurence.logisland.timeseries.sax.SaxAnalyzer.saxThreshold

/**
 * Unit test for the sax coding anomalies
 * @author Mejdeddine Nemsi
 */
class SaxAnalyserTest extends Specification{
    @Unroll
    def "test anomaly detection in sax strings data"(){

        given:
        def listin = []

        def listout =[]
        def s1 = "dededdecccdcedeedccddcddccedeeeeeedceefedbdbccdcccp"//usual sensor output [49]
        def s2 ="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"//  one type character [-1]
        def s3 = "b"//one character output [-1]
        def s4 = "saxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxh"//pattern anomaly (pattern : sax) [50]
        def s5 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa"//one anomaly in one type character output [32,33]
        def s6 = "eeffddddddcdffffeeededdddcedcdfdeddcccdcdedcbbbabadcddcddcefeededcddccedddceddKcedcdcbcceeddddcceeded"//unusual output length [49, 77, 78]
        def s7 = ""//empty sensor output [-1]

        listin << s1 << s2 << s3 << s4 <<s5 << s6 <<s7
        for (i in listin){
            listout<<anomalyDetect(i, saxThreshold(i))
        }
        expect:
        listout == [[49],[-1],[-1],[50],[32,33],[49, 77, 78],[-1]]
    }
}