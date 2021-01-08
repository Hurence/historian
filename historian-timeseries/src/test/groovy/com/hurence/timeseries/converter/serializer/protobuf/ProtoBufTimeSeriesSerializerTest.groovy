/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.timeseries.converter.serializer.protobuf

import com.hurence.timeseries.compaction.Compression
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesSerializer
import com.hurence.timeseries.model.Measure
import com.hurence.timeseries.model.Measure
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Array
import java.text.DecimalFormat
import java.time.Instant
import java.util.zip.GZIPInputStream

/**
 * Unit test for the protocol buffers serializer (points without quality)
 * @author f.lautenschlager
 */
class ProtoBufTimeSeriesSerializerTest extends Specification {

    private static Logger LOGGER = LoggerFactory.getLogger(ProtoBufTimeSeriesSerializerTest.class);

    def "test from without range query"() {
        given:
        def points = []
        100.times {
            points.add( Measure.fromValue(it, it * 100))
        }
        def compressedProtoPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())

        when:
        def uncompressedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(compressedProtoPoints), 0, points.size(), 0, points.size()))
        then:
        100.times {
            uncompressedPoints.get(it).getValue() == it * 100
            uncompressedPoints.get(it).getTimestamp() == it
        }
    }

    @Shared
    def start = Instant.now()
    @Shared
    def end = start.plusSeconds(100 * 100)

    def "test from with range query"() {
        given:
        def points = []

        100.times {
            points.add(Measure.fromValue(start.plusSeconds(it).toEpochMilli(), it * 100))
        }
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())

        when:
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), start.toEpochMilli(), end.toEpochMilli(), from, to)

        then:
        List<Measure> list = new ArrayList<>(uncompressedPoints)
        list.size() == size
        if (size == 21) {
            list.get(0).timestamp == 1456394850774
            list.get(0).value == 5000.0d

            list.get(20).timestamp == 1456394870774
            list.get(20).value == 7000.0d
        }

        where:
        from << [end.toEpochMilli() + 2, 0, start.toEpochMilli() + 4, start.plusSeconds(50).toEpochMilli()]
        to << [end.toEpochMilli() + 3, 0, start.toEpochMilli() + 2, start.plusSeconds(70).toEpochMilli()]
        size << [0, 0, 0, 21]
    }

    def "test convert to protocol buffers points"() {
        given:
        def points = []
        100.times {
            points.add(Measure.fromValue(it + 15, it * 100))
        }
        //Points that are null are ignored
        points.add(null)
        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 0, 114)

        then:
        uncompressedPoints.size() == 100
    }

    def "test iterator with invalid arguments"() {
        when:
        ProtoBufTimeSeriesSerializer.from(null, 0, 0, from, to)
        then:
        thrown IllegalArgumentException
        where:
        from << [-1, 0, -1]
        to << [0, -1, -1]

    }


    def "test private constructor"() {
        when:
        ProtoBufTimeSeriesSerializer.newInstance()
        then:
        noExceptionThrown()
    }

    def "test date-delta-compaction with almost_equals = 0"() {
        given:
        def points = []
        points.add(Measure.fromValue(1, 10))
        points.add(Measure.fromValue(5, 20))
        points.add(Measure.fromValue(8, 30))
        points.add(Measure.fromValue(16, 40))
        points.add(Measure.fromValue(21, 50))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())
        def uncompressedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1l, 1036l, 1l, 1036l))

        then:
        uncompressedPoints.get(0).timestamp == 1
        uncompressedPoints.get(1).timestamp == 5
        uncompressedPoints.get(2).timestamp == 8
        uncompressedPoints.get(3).timestamp == 16
        uncompressedPoints.get(4).timestamp == 21

    }

    def "test date-delta-compaction used in the paper"() {
        given:
        def points = []
        points.add(Measure.fromValue(1, 10))
        points.add(Measure.fromValue(5, 20))
        points.add(Measure.fromValue(8, 30))
        points.add(Measure.fromValue(16, 40))
        points.add(Measure.fromValue(21, 50))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 4)
        def uncompressedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1l, 1036l))

        then:
        uncompressedPoints.get(0).timestamp == 1//offset: 4
        uncompressedPoints.get(1).timestamp == 5//offset: 4
        uncompressedPoints.get(2).timestamp == 9//offset: 4
        uncompressedPoints.get(3).timestamp == 16//offset: 7
        uncompressedPoints.get(4).timestamp == 21//offset: 7

    }


    def "test date-delta-compaction"() {
        given:
        def points = []
        points.add(Measure.fromValue(10, -10))
        points.add(Measure.fromValue(20, -20))
        points.add(Measure.fromValue(30, -30))
        points.add(Measure.fromValue(39, -39))
        points.add(Measure.fromValue(48, -48))
        points.add(Measure.fromValue(57, -57))
        points.add(Measure.fromValue(66, -66))
        points.add(Measure.fromValue(75, -75))
        points.add(Measure.fromValue(84, -84))
        points.add(Measure.fromValue(93, -93))
        points.add(Measure.fromValue( 102, -102))
        points.add(Measure.fromValue( 111, -109))
        points.add(Measure.fromValue( 120, -118))
        points.add(Measure.fromValue( 129, -127))
        points.add(Measure.fromValue( 138, -136))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 10)
        def uncompressedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 10l, 1036l))

        then:                            //diff to origin
        uncompressedPoints.get(0).timestamp == 10//0
        uncompressedPoints.get(1).timestamp == 20//0
        uncompressedPoints.get(2).timestamp == 30//0
        uncompressedPoints.get(3).timestamp == 40//1
        uncompressedPoints.get(4).timestamp == 50//2
        uncompressedPoints.get(5).timestamp == 60//3
        uncompressedPoints.get(6).timestamp == 70//4
        uncompressedPoints.get(7).timestamp == 75//5 drift detected OK

        uncompressedPoints.get(8).timestamp == 84//9
        uncompressedPoints.get(9).timestamp == 93//9
        uncompressedPoints.get(10).timestamp == 102//9
        uncompressedPoints.get(11).timestamp == 111//9
        uncompressedPoints.get(12).timestamp == 120//9
        uncompressedPoints.get(13).timestamp == 129//9
        uncompressedPoints.get(14).timestamp == 138//9
    }


    def "test date-delta-compaction with different values"() {
        given:
        def points = []
        points.add(Measure.fromValue(1462892410, 10))
        points.add(Measure.fromValue(1462892420, 20))
        points.add(Measure.fromValue(1462892430, 30))
        points.add(Measure.fromValue(1462892439, 39))
        points.add(Measure.fromValue(1462892448, 48))
        points.add(Measure.fromValue(1462892457, 57))
        points.add(Measure.fromValue(1462892466, 66))
        points.add(Measure.fromValue(1462892475, 10))
        points.add(Measure.fromValue(1462892484, 84))
        points.add(Measure.fromValue(1462892493, 93))
        points.add(Measure.fromValue( 1462892502, -102))
        points.add(Measure.fromValue( 1462892511, 109))
        points.add(Measure.fromValue( 1462892520, 118))
        points.add(Measure.fromValue( 1462892529, 127))
        points.add(Measure.fromValue( 1462892538, 136))


        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 10)
        def uncompressedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1462892410L, 1462892538L))
        def listPoints = uncompressedPoints

        then:                            //diff to origin
        listPoints.get(0).timestamp == 1462892410//0
        listPoints.get(0).value == 10//0

        listPoints.get(1).timestamp == 1462892420//0
        listPoints.get(1).value == 20//0

        listPoints.get(2).timestamp == 1462892430//0
        listPoints.get(2).value == 30//0

        listPoints.get(3).timestamp == 1462892440//1
        listPoints.get(3).value == 39//0

        listPoints.get(4).timestamp == 1462892450//2
        listPoints.get(4).value == 48//0

        listPoints.get(5).timestamp == 1462892460//3
        listPoints.get(5).value == 57//0

        listPoints.get(6).timestamp == 1462892470//4
        listPoints.get(6).value == 66//0

        listPoints.get(7).timestamp == 1462892475//5
        listPoints.get(7).value == 10//0

        listPoints.get(8).timestamp == 1462892484//5
        listPoints.get(8).value == 84//0

        listPoints.get(9).timestamp == 1462892493//5
        listPoints.get(9).value == 93//0

        listPoints.get(10).timestamp == 1462892502//5
        listPoints.get(10).value == -102//0

        listPoints.get(11).timestamp == 1462892511//5
        listPoints.get(11).value == 109//0

        listPoints.get(12).timestamp == 1462892520//5
        listPoints.get(12).value == 118//0

        listPoints.get(13).timestamp == 1462892529//5
        listPoints.get(13).value == 127//0

        listPoints.get(14).timestamp == 1462892538//0
        listPoints.get(14).value == 136//0

    }

    def "test rearrange points"() {
        given:
        def points = []
        points.add(Measure.fromValue(100, 10))
        points.add(Measure.fromValue(202, 20))
        points.add(Measure.fromValue(305, 30))
        points.add(Measure.fromValue(401, 39))
        points.add(Measure.fromValue(509, 48))
        points.add(Measure.fromValue(510, 48))
        points.add(Measure.fromValue(511, 48))
        points.add(Measure.fromValue(509, 10))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 10)
        def uncompressedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 100L, 510L))
        def listPoints = uncompressedPoints

        then:
        listPoints.get(7).timestamp == 509

    }

    def "test ddc threshold -1"() {
        when:
        ProtoBufTimeSeriesSerializer.to(Collections.emptyList(), -1)
        then:
        thrown(IllegalArgumentException)
    }

    void writeToFile(file, message) {
        file.append(message)
        file.append("\n")
    }

    @Unroll
    def "test raw time series with almost_equals = #almostEquals"() {
        given:
        def rawTimeSeriesList = readTimeSeriesData()

        when:
        rawTimeSeriesList.each {
            LOGGER.trace( "Checking file ${it.key}")
            def points = it.value;
            points.sort()
            def startTs = points.get(0).getTimestamp()
            def endTs = points.get(points.size() - 1).getTimestamp()

            List<Measure> list = points;
            List<Measure> uniqueTS = new ArrayList<>();


            def prevDate = list.get(0).timestamp;

            for (int i = 1; i < list.size(); i++) {

                def currentDate = list.get(i).timestamp

                if (currentDate != prevDate) {
                    uniqueTS.add(Measure.fromValue(currentDate, list.get(i).value));
                }
                prevDate = currentDate

            }

            uniqueTS.sort()

            def start = System.currentTimeMillis()
            def serializedPoints = ProtoBufTimeSeriesSerializer.to(uniqueTS.iterator(), almostEquals)
            def end = System.currentTimeMillis()

            LOGGER.trace( "Serialization took ${end - start} ms")

//            def builder = new MetricTimeSeries.Builder("heap", "metric")

            start = System.currentTimeMillis()
            def unprotobuffedPoints = new ArrayList<>(ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), startTs, endTs))
            end = System.currentTimeMillis()

            LOGGER.trace( "Deserialization took ${end - start} ms")
//            def modifiedTimeSeries = builder.build()

            def count = uniqueTS.size();

            for (int i = 0; i < count; i++) {
                if (unprotobuffedPoints.get(i).getTimestamp() - uniqueTS.get(i).getTimestamp() > almostEquals) {
                    LOGGER.trace(("Position ${i}: Time diff is ${unprotobuffedPoints.get(i).getTimestamp() - uniqueTS.get(i).getTimestamp()}. Orginal ts: ${uniqueTS.get(i).getTimestamp()}. Reconstructed ts: ${unprotobuffedPoints.get(i).getTimestamp()}"))
                }
            }
        }
        then:
        noExceptionThrown()

        where:
        almostEquals << [10]

    }


    static def readTimeSeriesData() {
        def url = ProtoBufTimeSeriesSerializerTest.getResource("/data-mini")
        def tsDir = new File(url.toURI())

        def documents = new HashMap<String, List<Measure>>()

        tsDir.listFiles().each { File file ->
            LOGGER.trace(("Processing file $file"))
            documents.put(file.name, new ArrayList<Measure>())

            def nf = DecimalFormat.getInstance(Locale.ENGLISH)

            def unzipped = new GZIPInputStream(new FileInputStream(file))

            unzipped.splitEachLine(";") { fields ->
                //Its the first line of a csv file
                if ("Date" != fields[0]) {
                    //First field is the timestamp: 26.08.2013 00:00:17.361
                    def date = Instant.parse(fields[0])
                    fields.subList(1, fields.size()).eachWithIndex { String value, int i ->
                        documents.get(file.name).add(Measure.fromValue(date.toEpochMilli(), nf.parse(value).doubleValue()))
                    }
                }
            }

        }
        documents
    }
}
