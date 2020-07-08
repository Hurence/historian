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
import com.hurence.timeseries.modele.PointImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Constructor
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
            points.add(new PointImpl(it, it * 100))
        }
        def compressedProtoPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())

        when:
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(compressedProtoPoints), 0, points.size(), 0, points.size())
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
            points.add(new PointImpl(start.plusSeconds(it).toEpochMilli(), it * 100))
        }
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())

        when:
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), start.toEpochMilli(), end.toEpochMilli(), from, to)

        then:
        List<PointImpl> list = uncompressedPoints
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
            points.add(new PointImpl(it + 15, it * 100))
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
        points.add(new PointImpl(1, 10))
        points.add(new PointImpl(5, 20))
        points.add(new PointImpl(8, 30))
        points.add(new PointImpl(16, 40))
        points.add(new PointImpl(21, 50))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1l, 1036l, 1l, 1036l)

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
        points.add(new PointImpl(1, 10))
        points.add(new PointImpl(5, 20))
        points.add(new PointImpl(8, 30))
        points.add(new PointImpl(16, 40))
        points.add(new PointImpl(21, 50))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 4)
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1l, 1036l)

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
        points.add(new PointImpl(10, -10))
        points.add(new PointImpl(20, -20))
        points.add(new PointImpl(30, -30))
        points.add(new PointImpl(39, -39))
        points.add(new PointImpl(48, -48))
        points.add(new PointImpl(57, -57))
        points.add(new PointImpl(66, -66))
        points.add(new PointImpl(75, -75))
        points.add(new PointImpl(84, -84))
        points.add(new PointImpl(93, -93))
        points.add(new PointImpl( 102, -102))
        points.add(new PointImpl( 111, -109))
        points.add(new PointImpl( 120, -118))
        points.add(new PointImpl( 129, -127))
        points.add(new PointImpl( 138, -136))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 10)
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 10l, 1036l)

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
        points.add(new PointImpl(1462892410, 10))
        points.add(new PointImpl(1462892420, 20))
        points.add(new PointImpl(1462892430, 30))
        points.add(new PointImpl(1462892439, 39))
        points.add(new PointImpl(1462892448, 48))
        points.add(new PointImpl(1462892457, 57))
        points.add(new PointImpl(1462892466, 66))
        points.add(new PointImpl(1462892475, 10))
        points.add(new PointImpl(1462892484, 84))
        points.add(new PointImpl(1462892493, 93))
        points.add(new PointImpl( 1462892502, -102))
        points.add(new PointImpl( 1462892511, 109))
        points.add(new PointImpl( 1462892520, 118))
        points.add(new PointImpl( 1462892529, 127))
        points.add(new PointImpl( 1462892538, 136))


        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 10)
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 1462892410L, 1462892538L)
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
        points.add(new PointImpl(100, 10))
        points.add(new PointImpl(202, 20))
        points.add(new PointImpl(305, 30))
        points.add(new PointImpl(401, 39))
        points.add(new PointImpl(509, 48))
        points.add(new PointImpl(510, 48))
        points.add(new PointImpl(511, 48))
        points.add(new PointImpl(509, 10))

        when:
        def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), 10)
        def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), 100L, 510L)
        def listPoints = uncompressedPoints

        then:
        listPoints.get(7).timestamp == 509

    }

    def "test ddc threshold -1"() {
        when:
        ProtoBufTimeSeriesSerializer.to(null, -1)
        then:
        thrown(IllegalArgumentException)
    }

    def "test raw time series with almost_equals = 0"() {
        given:
        def rawTimeSeriesList = readTimeSeriesData()

        when:
        rawTimeSeriesList.each {
            LOGGER.trace("Checking file ${it.key}")
            def points = it.value
            points.sort()
            def startTs = points.get(0).getTimestamp()
            def endTs = points.get(points.size() - 1).getTimestamp()

            def start = System.currentTimeMillis()
            def serializedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator())
            def end = System.currentTimeMillis()

            LOGGER.trace("Serialization took ${end - start} ms")

            start = System.currentTimeMillis()
            def uncompressedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), startTs, endTs)
            end = System.currentTimeMillis()
            LOGGER.trace("Deserialization took ${end - start} ms")
            def count = points.size()
            LOGGER.trace("Checking $count points for almost_equals = 0")

            for (int i = 0; i < count; i++) {
                if (points.get(i).getTimestamp() != uncompressedPoints.get(i).getTimestamp()) {
                    long delta = points.get(i).getTimestamp() - uncompressedPoints.get(i).getTimestamp()
                    throw new IllegalStateException("Points are not equals at " + i + ". Should " + points.get(i).getTimestamp() + " but is " + uncompressedPoints.get(i).getTimestamp() + " a delta of " + delta)
                }
                if (points.get(i).getValue() != uncompressedPoints.get(i).getValue()) {
                    double delta = points.get(i).getValue() - uncompressedPoints.get(i).getValue()
                    throw new IllegalStateException("Values not equals at " + i + ". Should " + points.get(i).getValue() + " but is " + uncompressedPoints.get(i).getValue() + " a delta of " + delta)
                }
            }
        }
        then:
        noExceptionThrown()

    }

    def "test change rate"() {

        given:
        def rawTimeSeriesList = readTimeSeriesData()


        when:
        def file = new File("target/ddc-threshold.csv")
        file.write("")
        writeToFile(file, "Index;AlmostEquals;Legend;Value")

        def index = 1

        [0, 10, 25, 50, 100].eachWithIndex { def almostEquals, def almostEqualsIndex ->

            LOGGER.trace( "=============================================================" )
            LOGGER.trace( "===================     ${almostEquals} ms       =========================")
            LOGGER.trace( "=============================================================")

            def changes = 0
            def sumOfPoint = 0
            def maxDeviation = 0
            def indexwiseDeltaRaw = 0
            def indexwiseDeltaMod = 0
            def averageDeviation = 0

            def totalBytes = 0
            def totalSerializedBytes = 0


            rawTimeSeriesList.each { filename, points ->

                points.sort()
                def startTs = points.get(0).getTimestamp()
                def endTs = points.get(points.size() - 1).getTimestamp()

                def changesTS = 0
                def sumOfPointTS = 0
                def maxDeviationTS = 0
                def indexwiseDeltaRawTS = 0
                def indexwiseDeltaModTS = 0
                def averageDeviationTS = 0
                def compressedBytes = 0
                def url = ProtoBufTimeSeriesSerializerJavaTest.getResource("/data-mini")
                def tsDir = new File(url.toURI())
                def compressedFile = new File(tsDir, filename)
                def rawBytes = new GZIPInputStream(new FileInputStream(compressedFile)).getBytes().length

                def probuffedPoints = ProtoBufTimeSeriesSerializer.to(points.iterator(), almostEquals)

                def bytes = probuffedPoints.length
                compressedBytes = Compression.compress(probuffedPoints).length

                totalBytes += rawBytes
                totalSerializedBytes += bytes

                def unprotoBuffedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(probuffedPoints), startTs, endTs)

                sumOfPoint += points.size()
                sumOfPointTS = points.size()

                for (int j = 0; j < points.size(); j++) {

                    def rawTS = points.get(j).getTimestamp()
                    def modTS = unprotoBuffedPoints.get(j).getTimestamp()

                    def deviation = Math.abs(rawTS - modTS)

                    averageDeviationTS += deviation
                    averageDeviation += deviation

                    if (deviation > maxDeviation) {
                        maxDeviation = deviation
                    }

                    if (deviation > maxDeviationTS) {
                        maxDeviationTS = deviation
                    }

                    if (j + 1 < points.size()) {
                        indexwiseDeltaRaw += Math.abs(points.get(j + 1).getTimestamp() - rawTS)
                        indexwiseDeltaMod += Math.abs(unprotoBuffedPoints.get(j + 1).getTimestamp() - modTS)

                        indexwiseDeltaRawTS += indexwiseDeltaRaw
                        indexwiseDeltaModTS += indexwiseDeltaMod
                    }

                    if (rawTS != modTS) {
                        changes++
                        changesTS++
                    }
                }
                LOGGER.trace("=======================================================")
                LOGGER.trace("TS start: ${Instant.ofEpochMilli(startTs)} end: ${Instant.ofEpochMilli(endTs)}")
                LOGGER.trace("TS-MOD start: ${Instant.ofEpochMilli(unprotoBuffedPoints.get(0).getTimestamp())} end: ${Instant.ofEpochMilli(unprotoBuffedPoints.get(unprotoBuffedPoints.size() - 1).getTimestamp())}")
                LOGGER.trace("Max deviation: $maxDeviationTS in milliseconds")
                LOGGER.trace("Raw: Sum of deltas: $indexwiseDeltaRawTS in minutes")
                LOGGER.trace("Mod: Sum of deltas: $indexwiseDeltaModTS in minutes")
                LOGGER.trace("Change rate per point: ${changesTS / sumOfPointTS}")
                LOGGER.trace("Average deviation per point: ${averageDeviationTS / sumOfPointTS}")
                LOGGER.trace("Bytes after serialization: $bytes")
                LOGGER.trace("Bytes before serialization: $rawBytes")
                LOGGER.trace("Safes: ${(1 - bytes / rawBytes) * 100} %")
                LOGGER.trace("Bytes per point: ${bytes / sumOfPointTS}")
                LOGGER.trace("Compressed Bytes per point: ${compressedBytes / sumOfPointTS}")
                LOGGER.trace("=======================================================")
            }
            LOGGER.trace("=======================================================")
            LOGGER.trace("= Overall almost equals: $almostEquals")
            LOGGER.trace("=======================================================")
            LOGGER.trace("Max deviation: $maxDeviation in milliseconds")
            LOGGER.trace("Raw: Sum of deltas: $indexwiseDeltaRaw in minutes")
            LOGGER.trace("Mod: Sum of deltas: $indexwiseDeltaMod in minutes")
            LOGGER.trace("Change rate per point: ${changes / sumOfPoint}")
            LOGGER.trace("Average deviation per point: ${averageDeviation / sumOfPoint}")


            def changeRate = (changes / sumOfPoint) * 100
            def compressionRate = (1 - (totalSerializedBytes / totalBytes)) * 100

            writeToFile(file, "${index};${almostEquals};Timstamp Change Rate;${changeRate}")
            writeToFile(file, "${index};${almostEquals};Compression Rate;${compressionRate}")
            index++

        }



        then:
        noExceptionThrown()
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

            List<PointImpl> list = points;
            List<PointImpl> uniqueTS = new ArrayList<>();


            def prevDate = list.get(0).timestamp;

            for (int i = 1; i < list.size(); i++) {

                def currentDate = list.get(i).timestamp

                if (currentDate != prevDate) {
                    uniqueTS.add(new PointImpl(currentDate, list.get(i).value));
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
            def unprotobuffedPoints = ProtoBufTimeSeriesSerializer.from(new ByteArrayInputStream(serializedPoints), startTs, endTs)
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
        def url = ProtoBufTimeSeriesSerializerJavaTest.getResource("/data-mini")
        def tsDir = new File(url.toURI())

        def documents = new HashMap<String, List<PointImpl>>()

        tsDir.listFiles().each { File file ->
            LOGGER.trace(("Processing file $file"))
            documents.put(file.name, new ArrayList<PointImpl>())

            def nf = DecimalFormat.getInstance(Locale.ENGLISH)

            def unzipped = new GZIPInputStream(new FileInputStream(file))

            unzipped.splitEachLine(";") { fields ->
                //Its the first line of a csv file
                if ("Date" != fields[0]) {
                    //First field is the timestamp: 26.08.2013 00:00:17.361
                    def date = Instant.parse(fields[0])
                    fields.subList(1, fields.size()).eachWithIndex { String value, int i ->
                        documents.get(file.name).add(new PointImpl(date.toEpochMilli(), nf.parse(value).doubleValue()))
                    }
                }
            }

        }
        documents
    }
}
