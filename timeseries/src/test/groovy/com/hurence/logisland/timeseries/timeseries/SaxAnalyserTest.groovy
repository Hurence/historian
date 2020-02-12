package com.hurence.logisland.timeseries.timeseries
import spock.lang.Specification
import spock.lang.Unroll

import static com.hurence.logisland.timeseries.sax.SaxAnalyzer.anomalyDetect
import static com.hurence.logisland.timeseries.sax.SaxAnalyzer.saxThreshold

class SaxAnalyserTest extends Specification{
    @Unroll
    def "test anomaly detection in sax strings data"(){

        given:
        def listin = []

        def listout =[]
        def s1 = "dededdecccdcedeedccddcddccedeeeeeedceefedbdbccdcccp"//usual sensor output
        def s2 ="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"//  one type character
        def s3 = "b"//one character output
        def s4 = "saxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxsaxh"//pattern anomaly (pattern : sax)
        def s5 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa"//one anomaly in one type character output
        def s6 = "eeffddddddcdffffeeededdddcedcdfdeddcccdcdedcbbbabadcddcddcefeededcddccedddceddKcedcdcbcceeddddcceeded"//unusual output length

        listin << s1 << s2 << s3 << s4 <<s5 << s6
        for (i in listin){
            listout<<anomalyDetect(i, saxThreshold(i))
        }
        expect:
        listout == [[49],[-1],[-1],[50],[32,33],[49, 77, 78]]
    }
}