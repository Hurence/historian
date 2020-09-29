package com.hurence.historian.compactor;

import com.hurence.historian.compactor.config.Configuration;
import com.hurence.historian.compactor.config.ConfigurationBuilder;
import com.hurence.historian.compactor.config.ConfigurationException;
import com.hurence.historian.spark.ml.Chunkyfier;
import com.hurence.historian.spark.ml.UnChunkyfier;
import com.hurence.historian.spark.sql.Options;
import com.hurence.historian.spark.sql.reader.ChunksReaderType;
import com.hurence.historian.spark.sql.reader.ReaderFactory;
import com.hurence.historian.spark.sql.reader.solr.SolrChunksReader;
import com.hurence.historian.spark.sql.writer.WriterFactory;
import com.hurence.historian.spark.sql.writer.WriterType;
import com.hurence.historian.spark.sql.writer.solr.SolrChunksWriter;
import com.hurence.timeseries.core.ChunkOrigin;
import com.hurence.timeseries.model.Chunk;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hurence.timeseries.model.HistorianChunkCollectionFieldsVersionCurrent.*;

public class Compactor implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(Compactor.class);

    private static String configFilePath = null;
    private Configuration configuration = null;
    private SolrChunksReader solrChunksReader = null;
    private SolrChunksWriter solrChunksWriter = null;
    private SolrClient solrClient = null;
    private SparkSession sparkSession = null;
    private ScheduledExecutorService scheduledThreadPool = null;
    // Number of seconds before next compaction algorithm run
    private volatile int period = -1;
    // Should the first compaction algorithm run occur right after start?
    private volatile boolean startNow = true;
    private volatile boolean started = false;
    private volatile boolean closed = false;

    /**
     *
     * @param configuration
     */
    public Compactor(Configuration configuration) {
        this.configuration = configuration;
        initialize();
    }

    /**
     * Start the periodic run of the compaction
     */
    public synchronized void start() {

        if (closed) {
            throw new IllegalStateException("Cannot call start on closed compactor");
        }

        if (started) {
            logger.info("Attempt to start compactor while already started, doing nothing");
            return;
        }

        logger.info("Starting compactor using configuration: " + configuration);

        /**
         * Start looping running the compaction algorithm
         */

        // Need only one thread running the compaction
        scheduledThreadPool = Executors.newScheduledThreadPool(1);

        // Will the first algorithm run now of after a delay?
        int delay = period; // First run will start in period seconds from now
        if (startNow) {
            delay = 0; // First run will occur now
        }

        logger.info("Compactor starting in " + delay + " seconds and then every " + period + " seconds");

        scheduledThreadPool.scheduleWithFixedDelay(this, delay, period, TimeUnit.SECONDS);

        started = true;
        logger.info("Compactor started");
    }

    /**
     * Run the compaction
     */
    public void run() {

        if (closed) {
            throw new IllegalStateException("Cannot call run on closed compactor");
        }

        doCompact();
    }

    /**
     * Stops the compactor
     */
    public synchronized void stop() {

        if (closed) {
            throw new IllegalStateException("Cannot call stop on closed compactor");
        }

        if (!started) {
            logger.info("Attempt to stop compactor while already stopped, doing nothing");
            return;
        }

        logger.info("Stopping compactor...");

        scheduledThreadPool.shutdown();
        try {
            scheduledThreadPool.awaitTermination(3600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error while waiting for compactor to stop: " + e.getMessage());
            return;
        }

        started = false;
        logger.info("Compactor stopped");
    }

    private void closeSparkEnv() {
        sparkSession.close();
    }

    private void closeSolrClient() {
        try {
            solrClient.close();
        } catch (IOException e) {
            logger.error("Error closing solr client: " + e.getMessage());
        }
    }

    /**
     * Initializes the spark session according to the configuration
     */
    private void initSparkEnv() {

        SparkSession.Builder sessionBuilder = SparkSession.builder();

        Map<String, String> sparkConfig = configuration.getSparkConfig();

        if (sparkConfig.size() == 0) {
            logger.info("No spark options");
        } else {
            // Apply spark config entries defined in the configuration
            StringBuilder sb = new StringBuilder();
            configuration.getSparkConfig().forEach((configKey, configValue) -> {
                sessionBuilder.config(configKey, configValue);
                sb.append("\n" + configKey + ": " + configValue);
            });
            logger.info("Using the following spark options:" + sb);
        }

        sparkSession = sessionBuilder.getOrCreate();
    }

    /**
     * Initialize some variables once for all in the Compactor's life
     */
    private void initialize() {

        initSparkEnv();
        initSolrClient();

        solrChunksReader = (SolrChunksReader) ReaderFactory.getChunksReader(ChunksReaderType.SOLR());
        solrChunksWriter = (SolrChunksWriter) WriterFactory.getChunksWriter(WriterType.SOLR());

        // Copy once for all scheduling info that may be later changed in compactor lifecycle, for instance
        // with a REST service API to control the compactor...
        setPeriod(configuration.getCompactionSchedulingPeriod());
        setStartNow(configuration.isCompactionSchedulingStartNow());
    }



    /**
     * Make this compactor close all used underlying resources (end of life).
     * Cannot call start/stop or any other business method on this object after
     * calling this method. A new Compactor object should be re-created for this.
     */
    public synchronized void close() {
        closeSolrClient();
        closeSparkEnv();
        closed = true;
    }

    private void initSolrClient() {

        List<String> zkHosts = Arrays.asList(configuration.getSolrZkHost());
        CloudSolrClient.Builder solrClientBuilder =
                new CloudSolrClient.Builder(zkHosts, Optional.empty());
        solrClient = solrClientBuilder.build();
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public boolean isStartNow() {
        return startNow;
    }

    public void setStartNow(boolean startNow) {
        this.startNow = startNow;
    }

    /**
     * Get the first timestamp (in milliseconds) of the current day in UTC format.
     * For instance if you call this method on Friday 18 September 2020 in the middle of the day,
     * this should return 1600387200000 which corresponds to GMT: Friday 18 September 2020 00:00:00
     * @return
     */
    private static long utcFirstTimestampOfTodayMs() {

        // Get current time in UTC timezone
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);
        calendar.set(year, month, day, 0, 0, 0); // Remove hour, minute and seconds of the day from the time

        // This may return 1600387200388 timestamp in milliseconds which is GMT: Friday 18 September 2020 00:00:00.388
        long timestampMillis = calendar.getTime().getTime();

        // Remove the milliseconds by rounding down to number of seconds and passing again into ms
        // 1600387200388 -> 1600387200000

        return (timestampMillis / 1000L) * 1000L;
    }

    /**
     * Get not yet compacted chunks and compact them with potentially already compacted chunks
     * for the same day.
     */
    void doCompact() {

        logger.debug("Running compaction algorithm");

        Map<String, String> options = new HashMap<String, String>();
        options.put("zkhost", configuration.getSolrZkHost());
        options.put("collection", configuration.getSolrCollection());

        /**
         * Prepare query that gets documents (and operator):
         * - from epoch since yesterday (included)
         * - with origin not from compactor (chunks not already compacted and thus needing to be)
         * Return only interesting fields:
         * - metric_key
         * - chunk_day
         * Documentation for query parameters of spark-solr: https://github.com/lucidworks/spark-solr#query-parameters
         * Example:
         *
         * chunk_start:[* TO 1600387199999]
         * AND -chunk_origin:compactor
         * fields metric_key,chunk_day
         * rows 1000
         * request_handler /export
         */
        String query = CHUNK_START + ":[* TO " + (utcFirstTimestampOfTodayMs()-1L)+ "]" // Chunks from epoch to yesterday (included)
                + "AND -" + CHUNK_ORIGIN + ":" + ChunkOrigin.COMPACTOR; // Only not yet compacted chunks
        options.put("query", query);
        options.put("fields", METRIC_KEY + "," + CHUNK_DAY); // Return only metric_key and chunk_day fields
        options.put("rows", "1000");
        // Specify to use the /export handler instead of the /select so that we don't loose any chunk
        // (we do not have to specify a max_rows parameter)
        options.put("request_handler", "/export");

        Dataset<Row> resultDs = sparkSession.read()
                .format("solr")
                .options(options)
                .load();

        resultDs.show(100, false);
        System.out.println("#rows" + resultDs.count());

        resultDs = resultDs.distinct();

        resultDs.show(100, false);
        System.out.println("#rows" + resultDs.count());

        resultDs = resultDs.sort(resultDs.col(METRIC_KEY), resultDs.col(CHUNK_DAY));

        resultDs.show(100, false);
        System.out.println("#rows" + resultDs.count());

        for (Row row : resultDs.collectAsList()) {
            String metricKey = row.getAs(METRIC_KEY);
            String day = row.getAs(CHUNK_DAY);
            reCompact(metricKey, day);
        }

    }

    /**
     * Prepare a new options map for solr reading/writing, pre-filled with
     * connection/collection information and where only query related parameters
     * are still to be filled
     */
    private Map<String, String> newOptions() {

        Map<String, String> options = new HashMap<String, String>();
        options.put("zkhost", configuration.getSolrZkHost());
        options.put("collection", configuration.getSolrCollection());
        return options;
    }

    /**
     * Recompact chunks of a metric for a day
     * @param metricKeyStr
     * @param day
     */
    private void reCompact(String metricKeyStr, String day) {

        logger.debug("Re-compacting chunks of day " + day + " for metric " + metricKeyStr);

        /**
         * Load all chunks (whether already compacted or not) of the day for the
         * metric
         */

        Map<String, String> options = newOptions();

        /**
         * chunk_day:2020-08-28
         * AND metric_key:metric1,dataCenter=1,room=1
         */
        String query = CHUNK_DAY + ":" + day +
                " AND " + METRIC_KEY + ":" + metricKeyStr;
        options.put("query", query);
        options.put("rows", "1000");
        options.put("max_rows", "1000");
        // Specify to use the /export handler instead of the /select so that we don't loose any chunk
        // (we do not have to specify a max_rows parameter)
        // TODO: to use /export, declare all chunk fields as docValues in solr, if not using
        // /export but /select. What about max_rows value ?
        //options.put("request_handler", "/export");

        // Compute and set the metric name and tags to read from the metric key
        Chunk.MetricKey metricKey = Chunk.MetricKey.parse(metricKeyStr);
        Set<String> tags = metricKey.getTagKeys();
        String metricAndTagsCsv = NAME + "," + tags.stream().collect(Collectors.joining(","));
        options.put(Options.TAG_NAMES(), metricAndTagsCsv);

        System.out.println("####################################### " + metricAndTagsCsv);

        // JavaConverters used to convert from java Map to scala immutable Map
        Options readerOptions = new Options(configuration.getSolrCollection(), JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
                Predef.<Tuple2<String, String>>conforms()));
        Dataset<Chunk> chunksToRecompact = (Dataset<Chunk>)solrChunksReader.read(readerOptions);

        chunksToRecompact.cache();

        System.out.println("------------------------------------- " + day + " for metric " + metricKey + " " +
                chunksToRecompact.count() + " chunks:");
        chunksToRecompact.show(100, false);

        /**
         * Save ids of the read chunks to recompact
         */

//        JavaRDD<String> chunksToRecompactIds = chunksToRecompact.toJavaRDD().map(chunk -> chunk.getId());
        List<String> chunksToRecompactIds = chunksToRecompact.toJavaRDD().map(chunk -> chunk.getId()).collect();
//        List<String> chunksToRecompactIds = new ArrayList<String>();

        /**
         * Unchunkyfy the read chunks
         */

        UnChunkyfier unchunkyfier = new UnChunkyfier();
        Dataset<Row> metricsToRecompact = unchunkyfier.transform(chunksToRecompact);

        System.out.println("Unchunkyfied metric values count " + metricsToRecompact.count() + ":");

        metricsToRecompact.show(100, false);

        /**
         * Re-compact those chunks
         */

        // Compute ["name", "tag1", "tag2"] String array
        List<String> groupByCols = new ArrayList<String>();
        groupByCols.add(NAME);
        groupByCols.addAll(tags.stream().map(tag -> "tags." + tag).collect(Collectors.toList()));
        String[] groupByColsArray = new String[groupByCols.size()];
        groupByColsArray = groupByCols.toArray(groupByColsArray);
        System.out.println("############################# groupByColsArray: " + groupByColsArray);

        Chunkyfier chunkyfier = new Chunkyfier()
                .setValueCol("value")
                .setQualityCol("quality")
                .setOrigin(ChunkOrigin.COMPACTOR.toString())
                .setTimestampCol("timestamp")
                .setGroupByCols(groupByColsArray)
                .setDateBucketFormat("yyyy-MM-dd")
                .setSaxAlphabetSize(7)
                .setSaxStringLength(50);
        Dataset<Row> recompactedChunksRows = chunkyfier.transform(metricsToRecompact);

        System.out.println("Recompacted chunks count " + recompactedChunksRows.count() + ":");

        recompactedChunksRows.show(100, false);

        /**
         * Write new re-compacted chunks
         */

        options = newOptions();
        options.put(Options.TAG_NAMES(), metricAndTagsCsv);
        // JavaConverters used to convert from java Map to scala immutable Map
        Options writerOptions = new Options(configuration.getSolrCollection(), JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
                Predef.<Tuple2<String, String>>conforms()));

        Dataset<Chunk> recompactedChunks = recompactedChunksRows
                .as(Encoders.bean(Chunk.class));
        solrChunksWriter.write(writerOptions, recompactedChunks);

        // TODO commit written docs? -> may be not needed as solrchunkwriter performs save operation under the hood

        /**
         * Delete old chunks
         */

        // TODO transactional way? be sure written is done before deleting.....
        deleteDocuments(chunksToRecompactIds);
    }

    /**
     * Delete documents with the passed ids
     * @param documentsToDelete
     */
    private void deleteDocuments(List<String> documentsToDelete) {

        for (String id : documentsToDelete) {

            System.out.println("Deleting document id " + id);

            try {
                solrClient.deleteById(configuration.getSolrCollection(), id);
            } catch (Exception e) {
                logger.error("Error deleting chunk document with id " + id + ": " + e.getMessage());
            }
        }

        try {
            solrClient.commit(configuration.getSolrCollection(), true, true);
        } catch (Exception e) {
            logger.error("Error committing deleted chunks: " + e.getMessage());
        }
    }

    /**
     * Loads the configuration file
     */
    private static Configuration loadConfigFile() {
        logger.info("Loading this configuration file: " + configFilePath);
        Configuration configuration = null;
        try {
            configuration = ConfigurationBuilder.load(configFilePath, logger);
        } catch (ConfigurationException e) {
            logger.error("Error loading configuration file: " + e.getMessage());
            System.exit(1);
        }

        return configuration;
    }

    /**
     * Compactor main entry point.
     * @param args
     */
    public static void main(String[] args) {

        parseCommandLine(args);
        Configuration configuration = loadConfigFile();
        Compactor compactor = new Compactor(configuration);
        compactor.start();
    }

    /**
     * Parses the command line
     */
    private static void parseCommandLine(String[] args)
    {
        // Config file path option
        final String OPTION_CONFIG_FILE = "c";
        final String OPTION_CONFIG_FILE_LONG = "config-file";

        // Handling arguments
        CommandLineParser parser = new DefaultParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

        // Path to config file
        Option configFileOption = new Option(OPTION_CONFIG_FILE, OPTION_CONFIG_FILE_LONG,
                        true, "Path to the YAML configuration file.");
        configFileOption.setRequired(true);

        options.addOption(configFileOption);

        /**
         * Get CLI options
         */

        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        } catch(Exception e)
        {
            displayUsageAndExitOnError(options, e.getMessage(), 1);
        }

        /**
         * Config file option.
         * Get and validate config file path.
         */
        configFilePath = line.getOptionValue(OPTION_CONFIG_FILE, null);
        if (configFilePath == null)
        {
            displayUsageAndExitOnError(options, "Must provide a config file path", 1);
        }

        logger.info("Using config file: " + configFilePath);
    }

    /**
     * Display the passed error message then exits with passed error code.
     * @param errorMsg
     */
    private static void displayUsageAndExitOnError(org.apache.commons.cli.Options options, String errorMsg, int errorCode)
    {
        System.out.println();
        System.out.println(errorMsg);
        displayUsage(options);
        System.exit(errorCode);
    }

    /**
     * Display program usage
     */
    private static void displayUsage(org.apache.commons.cli.Options options)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        String header = "Hurence Historian Compactor. Compactor compacts points of the same day in chunks.";
        String footer = "Developed by Hurence.";
        System.out.println();
        formatter.printHelp("Compactor", header, options, footer, true);
        System.out.println();
    }
}
