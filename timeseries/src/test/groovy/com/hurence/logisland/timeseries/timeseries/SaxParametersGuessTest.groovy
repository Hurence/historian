package com.hurence.logisland.timeseries.timeseries

import spock.lang.Specification
import spock.lang.Unroll


import static com.hurence.logisland.timeseries.sax.SaxParametersGuess.guess

/**git//git diff
 * Unit test for the sax parameter guess
 * @author Mejdeddine Nemsi
 */
class SaxParametersGuessTest extends Specification{
    @Unroll
    def "test parameter calculation data"(){
        given:
        def listin = new ArrayList<>()

        def listout =[]
        def s1 = [5.635,5.635,5.64,5.64,5.64,5.65,5.66,5.68,5.7,5.71,5.71,5.715,5.72,5.73,5.735,5.74,
            5.77,5.78,5.775,5.78,5.79,5.785,5.79,5.8,5.81,5.815,5.8,5.805,5.815,5.82,5.82,5.82,
            5.825,5.82,5.83,5.83,5.82,5.81,5.815,5.82,5.82,5.815,5.815,5.8,5.79,5.78,5.77]
        def s2 = []
        def s3 = [5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,
                  5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,
                  5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77,5.77]
        listin << s1 << s2 << s3
        for (i in listin){
            listout << guess(i as double[],0,46,2,2,10,1,2,10,1)
        }
        expect:
        listout == [[12, 4, 9, 1.4450012717343763, 100, 6, 100, 84, 2, 0.5531914893617021, 100, 0.3333333333333333, 2]
                    ,[]
                    ,[2, 2, 2, 5.095510249803914, 2, 1, 2, 2, 1, 0.0, 2, 1.0, 1]]
    }
}
