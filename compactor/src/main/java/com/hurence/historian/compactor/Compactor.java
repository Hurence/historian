package com.hurence.historian.compactor;

import com.hurence.historian.compactor.config.Configuration;
import com.hurence.historian.compactor.config.ConfigurationBuilder;
import com.hurence.historian.compactor.config.ConfigurationException;
import com.hurence.historian.spark.sql.reader.ChunksReaderType;
import com.hurence.historian.spark.sql.reader.ReaderFactory;
import com.hurence.historian.spark.sql.reader.solr.SolrChunksReader;
import com.hurence.historian.spark.sql.writer.WriterFactory;
import com.hurence.historian.spark.sql.writer.WriterType;
import com.hurence.historian.spark.sql.writer.solr.SolrChunksWriter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Compactor implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(Compactor.class);

    private static String configFilePath = null;
    private Configuration configuration = null;
    private SolrChunksReader solrChunksReader = null;
    private SolrChunksWriter solrChunksWriter = null;
    private ScheduledExecutorService scheduledThreadPool = null;
    // Number of seconds before next compaction algorithm run
    private volatile int period = -1;
    // Should the first compaction algorithm run occur right after start?
    private volatile boolean startNow = true;
    private volatile boolean started = false;

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

        doCompact();
    }

    /**
     * Stops the compactor
     */
    public synchronized void stop() {

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

    /**
     * Initialize some variables once for all in the Compactor life
     */
    private void initialize() {

        solrChunksReader = (SolrChunksReader) ReaderFactory.getChunksReader(ChunksReaderType.SOLR());
        solrChunksWriter = (SolrChunksWriter) WriterFactory.getChunksWriter(WriterType.SOLR());

        // Copy once for all scheduling info that may be later changed in compactor lifecycle, for instance
        // with a REST service API to control the compactor...
        setPeriod(configuration.getCompactionSchedulingPeriod());
        setStartNow(configuration.isCompactionSchedulingStartNow());
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

//        Map<String, String>  options = new HashMap<String, String>();
//        options.put("zkhost", zkHost);
//        options.put("collection", collectionName);
//        options.put("tag_names", "metric_id");
//        // JavaConverters used to convert from java Map to scala immutable Map
//        Options sqlOptions = new Options(collectionName, JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
//                Predef.<Tuple2<String, String>>conforms()));
//        Dataset<ChunkVersionCurrent> chunks = solrChunksReader.read(sqlOptions)
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
        Options options = new Options();

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
    private static void displayUsageAndExitOnError(Options options, String errorMsg, int errorCode)
    {
        System.out.println();
        System.out.println(errorMsg);
        displayUsage(options);
        System.exit(errorCode);
    }

    /**
     * Display program usage
     */
    private static void displayUsage(Options options)
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
