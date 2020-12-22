package com.hurence.historian.compactor;

import com.hurence.historian.compactor.config.Configuration;
import com.hurence.historian.compactor.config.ConfigurationBuilder;
import com.hurence.historian.compactor.config.ConfigurationException;
import com.hurence.historian.date.util.DateUtil;
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
import org.apache.commons.cli.*;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
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
import java.util.stream.Stream;

import static com.hurence.historian.model.HistorianChunkCollectionFieldsVersionCurrent.*;
import static com.hurence.historian.compactor.ZkUtils.*;

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

    // Set this flag to true when Compactor created from test code and we want
    // all spark parameters from configuration file being taken into account
    private boolean treatAllSparkParameters = false;

    /**
     *
     * @param configuration
     */
    public Compactor(Configuration configuration) {
        this(configuration, false);
    }
    /**
     *
     * @param configuration
     * @param treatAllSparkParameters
     */
    public Compactor(Configuration configuration, boolean treatAllSparkParameters) {
        this.treatAllSparkParameters = treatAllSparkParameters;
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

        try {
            doCompact();
        } catch (Throwable t) {
            logger.error("Error running compaction algorithm", t);
        }
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

        logger.info("Stopping compactor");

        scheduledThreadPool.shutdown();
        try {
            scheduledThreadPool.awaitTermination(3600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error while waiting for compactor to stop: " + e.getMessage());
            return;
        }

        close();

        started = false;
        logger.info("Compactor stopped");
    }

    private void closeSparkEnv() {
        logger.info("Closing spark session");
        sparkSession.close();
        logger.info("Spark session closed");
    }

    private void closeSolrClient() {
        try {
            logger.info("Closing solr client");
            solrClient.close();
            logger.info("Solr client closed");
        } catch (IOException e) {
            logger.error("Error closing solr client: " + e.getMessage());
        }
    }

    /**
     * List of spark configuration properties we must ignore from the
     * configuration file as they are read and set from the run script.
     * This can be a property or the beginning of a property (we use startsWith)
     * Configuration properties documentation:
     * https://spark.apache.org/docs/[version|latest]/configuration.html
     * i.e: https://spark.apache.org/docs/2.3.2/configuration.html
     */
    private static final Set<String> SPARK_PARAMS_HANDLED_BY_RUN_SCRIPT =
            Stream.of("spark.master",
                    "spark.submit.deployMode",
                    "spark.driver.", // Any driver parameter
                    "spark.executor.") // Any executor parameter
            .collect(Collectors.toSet());

    /**
     * Returns true if the passed spark parameter is treated by the run script
     * @param parameter
     * @return
     */
    private static boolean configTreatedByRunScript(String parameter) {
        for (String parameterStart : SPARK_PARAMS_HANDLED_BY_RUN_SCRIPT) {
            if (parameter.startsWith(parameterStart)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Initializes the spark session according to the configuration
     */
    private void initSparkEnv() {

        logger.info("Initializing spark session");

        SparkSession.Builder sessionBuilder = SparkSession.builder();

        Map<String, String> sparkConfig = configuration.getSparkConfig();

        if (sparkConfig.size() == 0) {
            logger.info("No spark options");
        } else {
            // Apply spark config entries defined in the configuration
            // except those handled by the run script
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : configuration.getSparkConfig().entrySet()) {
                String configKey = entry.getKey();
                String configValue = entry.getValue();
                if (treatAllSparkParameters || !configTreatedByRunScript(configKey)) {
                    sessionBuilder.config(configKey, configValue);
                    sb.append("\n" + configKey + ": " + configValue);
                } else {
                    logger.debug("Ignoring spark parameter handled by script: " +
                            configKey + "=" + configValue);
                }
            }
            logger.info("Using the following spark options:" + sb);
        }

        sparkSession = sessionBuilder.getOrCreate();
        logger.info("Spark session initialized");
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
        logger.info("Closing compactor");
        closeSolrClient();
        closeSparkEnv();
        closed = true;
    }

    private void initSolrClient() {

        logger.info("Initializing solr client");

        String zkHosts = configuration.getSolrZkHost();
        logger.debug("Parsing zk connect string: " + zkHosts);
        SolrZkConnectInfo solrZkConnectInfo = SolrZkConnectInfo.parseZkConnectString(zkHosts);
        logger.debug("Initializing solr client with: " + solrZkConnectInfo);

        Optional optionalChRoot = Optional.empty();
        String chRoot = solrZkConnectInfo.getZkChroot();
        if (chRoot != null) {
            optionalChRoot = Optional.of(chRoot);
        }

        CloudSolrClient.Builder solrClientBuilder =
                new CloudSolrClient.Builder(solrZkConnectInfo.getZkHosts(), optionalChRoot);
        solrClient = solrClientBuilder.build();
        logger.info("Solr client initialized");
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
     * Test if a dataset is empty
     * @return
     */
    private static boolean isEmpty(Dataset<Row> dataset) {

        Row[] firstRows = null;
        try {
            firstRows = (Row[])dataset.head(1);
        } catch(Throwable e) {
            logger.debug("Empty dataset: " + e.getMessage());
            return true;
        }

        if ( (firstRows == null) || (firstRows.length == 0) ) {
            logger.debug("No rows in dataset");
            return true;
        }

        return false;
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
         * Get old documents whose chunk origin is not compactor, starting from yesterday
         *
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
        String query = CHUNK_START + ":[* TO " + (DateUtil.utcFirstTimestampOfTodayMs()-1L)+ "]" // Chunks from epoch to yesterday (included)
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

        // Test emptiness (nothing to recompact or empty collection)
        if (isEmpty(resultDs)) {
            logger.debug("Nothing to re-compact");
            resultDs.unpersist();
            return;
        }

        /**
         * Sort by metric key then day
         */

        // One row per (metric key,day)
        resultDs = resultDs.distinct();

        // Sort per metric day then key
        resultDs = resultDs.sort( resultDs.col(CHUNK_DAY), resultDs.col(METRIC_KEY));


        /**
         * Re-compact each (metric key,day) chunks whether from injector or compactor
         */

        for (Row row : resultDs.collectAsList()) {
            String metricKey = row.getAs(METRIC_KEY);
            String day = row.getAs(CHUNK_DAY);
            reCompact(metricKey, day);
        }

        resultDs.unpersist();
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
        String tagsCsv = null;
        if (tags.size() > 0) {
            tagsCsv = tags.stream().collect(Collectors.joining(","));
            options.put(Options.TAG_NAMES(), tagsCsv);
        }

        // JavaConverters used to convert from java Map to scala immutable Map
        Options readerOptions = new Options(configuration.getSolrCollection(), JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
                Predef.<Tuple2<String, String>>conforms()));
        Dataset<Chunk> chunksToRecompact = (Dataset<Chunk>)solrChunksReader.read(readerOptions);

        chunksToRecompact.cache();

        //System.out.println("Chunks to recompact (" + chunksToRecompact.count() + " chunks):");
        //chunksToRecompact.show(100, false);

        /**
         * Save ids of the read chunks to recompact to delete them at the end
         */

        List<ChunkToDelete> chunksToDeletetIds = chunksToRecompact.toJavaRDD().map(chunk -> {
            String id = chunk.getId();
            String origin = chunk.getOrigin();
            return new ChunkToDelete(id, origin);
        }).collect();

        // Spread chunks to delete in 2 lists:
        // - chunks that come from compactor
        // - others
        List<String> compactorChunksToDeleteIds = new ArrayList<String>();
        List<String> nonCompactorChunksToDeleteIds = new ArrayList<String>();
        chunksToDeletetIds.forEach(chunkToDelete -> {
            if (chunkToDelete.origin.equals(ChunkOrigin.COMPACTOR.toString())) {
                compactorChunksToDeleteIds.add(chunkToDelete.getId());
            } else {
                nonCompactorChunksToDeleteIds.add(chunkToDelete.getId());
            }
        });

        /**
         * Unchunkyfy the read chunks
         */

        UnChunkyfier unchunkyfier = new UnChunkyfier();
        Dataset<Row> metricsToRecompact = unchunkyfier.transform(chunksToRecompact);

        //System.out.println("Metrics to recompact (" + metricsToRecompact.count() + " metrics):");
        //metricsToRecompact.show(100, false);

        /**
         * Re-compact those chunks
         */

        // Compute ["name", "tags.tag1", "tags.tag2"] String array
        List<String> groupByCols = new ArrayList<String>();
        groupByCols.add(NAME);
        groupByCols.addAll(tags.stream().map(tag -> "tags." + tag).collect(Collectors.toList()));
        String[] groupByColsArray = new String[groupByCols.size()];
        groupByColsArray = groupByCols.toArray(groupByColsArray);

        Chunkyfier chunkyfier = new Chunkyfier()
                .setValueCol("value")
                .setQualityCol("quality")
                .setOrigin(ChunkOrigin.COMPACTOR.toString())
                .setTimestampCol("timestamp")
                .setGroupByCols(groupByColsArray)
                .setDateBucketFormat(configuration.getDateBucketFormat());
        Dataset<Row> recompactedChunksRows = chunkyfier.transform(metricsToRecompact);

        //System.out.println("Recompacted chunks (" + recompactedChunksRows.count() + " chunks):");
        //recompactedChunksRows.show(100, false);

        /**
         * Write new re-compacted chunks
         */

        options = newOptions();
        if (tags.size() > 0) {
            options.put(Options.TAG_NAMES(), tagsCsv);
        }
        // JavaConverters used to convert from java Map to scala immutable Map
        Options writerOptions = new Options(configuration.getSolrCollection(),
                JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(Predef.<Tuple2<String, String>>conforms()));

        Dataset<Chunk> recompactedChunks = recompactedChunksRows
                .as(Encoders.bean(Chunk.class));
        solrChunksWriter.write(writerOptions, recompactedChunks);

        /**
         * Delete old chunks
         */

        deleteOldChunks(compactorChunksToDeleteIds, nonCompactorChunksToDeleteIds);

        recompactedChunks.unpersist();
        recompactedChunksRows.unpersist();
        metricsToRecompact.unpersist();
        chunksToRecompact.unpersist();
        compactorChunksToDeleteIds.clear();
        nonCompactorChunksToDeleteIds.clear();
    }

    /**
     * Deletes passed chunks
     * @param compactorChunksToDeleteIds
     * @param nonCompactorChunksToDeleteIds
     */
    private void deleteOldChunks(List<String> compactorChunksToDeleteIds,
                                 List<String>  nonCompactorChunksToDeleteIds) {

        // Delete first all origin=compactor chunks then origin=injector chunks
        // so that at least one origin=injector chunk is still there if deletion
        // fails in the middle of the processing of even before deleting old
        // chunks and in the middle of the process of writing new recompacted chunks.
        // That way, the next run of algo will detect still present
        // origin=injector chunks which will re-engage full recompaction for the
        // same day of both origin=compactor and origin=injector chunks. This will
        // retrieve old potential 'zombie' chunks and recompact them.
        // Note: this implies that the chunkyfier mechanism is able to handle 2
        // chunks with same point (timestamp,value) and combine them with only one.

        logger.debug("Deleting old chunks from compactor");
        deleteDocuments(compactorChunksToDeleteIds);
        logger.debug("Deleting old chunks not from compactor");
        deleteDocuments(nonCompactorChunksToDeleteIds);
    }

    /**
     * Delete documents with the passed ids
     * @param documentsToDelete
     */
    private void deleteDocuments(List<String> documentsToDelete) {

        logger.debug("Will delete " + documentsToDelete.size() + " documents");

        for (String id : documentsToDelete) {
            logger.debug("Deleting document id " + id);
            try {
                solrClient.deleteById(configuration.getSolrCollection(), id);
            } catch (Exception e) {
                logger.error("Error deleting chunk document with id " + id + ": " + e.getMessage());
            }
        }

        try {
            logger.debug("Committing documents deletion");
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
        compactor.waitForStop();
    }

    /**
     * Wait for the compactor to be stopped by someone else
     */
    public synchronized void waitForStop() {

        if (closed) {
            // Just stopped, nothing to do
            return;
        }

        if (!started) {
            // Not started, nothing to do
            return;
        }

        logger.info("Waiting for compactor to stop");
        while(true) {
            try {
                scheduledThreadPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted waiting for compactor to stop: " + e.getMessage());
            }
        }
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
        CommandLineParser parser = new GnuParser();
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
