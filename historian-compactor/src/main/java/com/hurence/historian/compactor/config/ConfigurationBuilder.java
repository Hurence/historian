package com.hurence.historian.compactor.config;

import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConfigurationBuilder {

    /**
     * Expected keys in the yaml file
     */
    private static final String KEY_SOLR = "solr";
    private static final String KEY_SOLR_ZKHOST = "zkHost";
    private static final String KEY_SOLR_COLLECTION = "collection";
    private static final String KEY_DATE_BUCKET_FORMAT = "date.bucket.format";

    private static final String KEY_SPARK = "spark";

    private static final String KEY_COMPACTION = "compaction";
    private static final String KEY_COMPACTION_SCHEDULING = "scheduling";
    private static final String KEY_COMPACTION_SCHEDULING_PERIOD = "period";
    private static final String KEY_COMPACTION_SCHEDULING_STARTNOW = "startNow";

    private String solrZkHost = null;
    private String solrCollection = null;
    private String dateBucketFormat = null;
    private int compactionSchedulingPeriod = -1;
    private boolean compactionSchedulingStartNow = true;
    private Map<String, String> sparkConfig = new HashMap<String, String>();


    public ConfigurationBuilder withDateBucketFormat(String dateBucketFormat) {
        this.dateBucketFormat = dateBucketFormat;
        return this;
    }

    public ConfigurationBuilder withSolrZkHost(String solrZkHost) {
        this.solrZkHost = solrZkHost;
        return this;
    }

    public ConfigurationBuilder withSolrCollection(String solrCollection) {
        this.solrCollection = solrCollection;
        return this;
    }

    public ConfigurationBuilder withCompactionSchedulingPeriod(int compactionSchedulingPeriod) {
        this.compactionSchedulingPeriod = compactionSchedulingPeriod;
        return this;
    }

    public ConfigurationBuilder withCompactionSchedulingStartNow(boolean compactionSchedulingStartNow) {
        this.compactionSchedulingStartNow = compactionSchedulingStartNow;
        return this;
    }

    public ConfigurationBuilder withSparkConfig(String configKey, String configValue) {
        sparkConfig.put(configKey, configValue);
        return this;
    }

    /**
     * Check the passed configuration entry is set and non empty.
     * Throws exception if any problem detected
     * @param keyName
     * @param keyValue
     * @throws ConfigurationException
     */
    private static void assertEntrySetAndNonEmpty(String keyName, String keyValue) throws ConfigurationException {
        if (keyValue == null) {
            throw new ConfigurationException("Missing " + keyName + " configuration entry");
        } else if (keyValue.trim().length() == 0) {
            throw new ConfigurationException("Empty " + keyName + " configuration entry");
        }
    }

    /**
     * Build the configuration object
     * @return
     */
    public Configuration build() throws ConfigurationException {

        // Sanity checking
        assertEntrySetAndNonEmpty(KEY_SOLR_ZKHOST, solrZkHost);
        assertEntrySetAndNonEmpty(KEY_SOLR_COLLECTION, solrCollection);

        // All ok, let's build the object.
        Configuration configuration = new Configuration();
        configuration.setSolrZkHost(solrZkHost);
        configuration.setSolrCollection(solrCollection);
        configuration.setCompactionSchedulingPeriod(compactionSchedulingPeriod);
        configuration.setCompactionSchedulingStartNow(compactionSchedulingStartNow);
        configuration.setSparkConfig(sparkConfig);
        configuration.setDateBucketFormat(dateBucketFormat);

        return  configuration;
    }

    /**
     * Loads the passed yaml configuration file as a configuration object
     * @param configFile
     * @return
     */
    public static Configuration load(String configFile, Logger logger) throws ConfigurationException {

        if ( (configFile == null) || (configFile.length() == 0 ) ) {
            throw new IllegalArgumentException("Null or empty configuration file");
        }

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        Yaml yaml = new Yaml();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(configFile));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Cannot open yaml configuration file: " + configFile);
        }

        Map<String, Object> config = yaml.load(fis);

        try {
            fis.close();
        } catch (IOException e) {
            if (logger != null) {
                logger.error("Error closing yaml file: " + e.getMessage());
            }
        }

        /**
         * Parse solr section
         */

        Object solrValues = config.get(KEY_SOLR);
        if (solrValues == null) {
            throw new ConfigurationException("Missing " + KEY_SOLR + " entry");
        }
        Map<String, Object> solr = (Map<String, Object>)solrValues;

        // solr zkHost
        Object solrZkHost = solr.get(KEY_SOLR_ZKHOST);
        if (solrZkHost == null) {
            throw new ConfigurationException("Missing " + KEY_SOLR_ZKHOST + " entry in " + KEY_SOLR + " entry");
        } else
        {
            configurationBuilder.withSolrZkHost((String)solrZkHost);
        }

        // solr collection name
        Object solrCollection = solr.get(KEY_SOLR_COLLECTION);
        if (solrCollection == null) {
            throw new ConfigurationException("Missing " + KEY_SOLR_COLLECTION + " entry in " + KEY_SOLR + " entry");
        } else
        {
            configurationBuilder.withSolrCollection((String)solrCollection);
        }

        /**
         * Parse spark section
         */

        Object sparkValues = config.get(KEY_SPARK);
        if (sparkValues == null) {
            throw new ConfigurationException("Missing " + KEY_SPARK + " entry");
        }
        Map<String, Object> spark = (Map<String, Object>)sparkValues;

        for (Map.Entry<String, Object> entry : spark.entrySet()) {
            String configKey = entry.getKey();
            Object configValue = entry.getValue();
          /*  if (!configKey.startsWith("spark.")) {
                throw new ConfigurationException(KEY_SPARK + " entry properties names must always start with 'spark.': " + configKey);
            }*/
            if (!(configValue instanceof String)) {
                throw new ConfigurationException(KEY_SPARK + " entry properties values must always be strings: " +
                        configKey + ": " + configValue + "(this is a " + configValue.getClass().getSimpleName() + ")");
            }
            configurationBuilder.withSparkConfig(configKey, (String)configValue);
        }

        /**
         * Parse compaction section
         */

        Object compactionValues = config.get(KEY_COMPACTION);
        if (compactionValues == null) {
            throw new ConfigurationException("Missing " + KEY_COMPACTION + " entry");
        }
        Map<String, Object> compaction = (Map<String, Object>)compactionValues;

        /**
         * Parse compaction.scheduling section
         */

        Object compactionSchedulingValues = compaction.get(KEY_COMPACTION_SCHEDULING);
        if (compactionSchedulingValues == null) {
            throw new ConfigurationException("Missing " + KEY_COMPACTION_SCHEDULING + " entry in " + KEY_COMPACTION + " entry");
        }
        Map<String, Object> compactionScheduling = (Map<String, Object>)compactionSchedulingValues;

        // compaction scheduling period
        Object compactionSchedulingPeriod = compactionScheduling.get(KEY_COMPACTION_SCHEDULING_PERIOD);
        if (compactionSchedulingPeriod == null) {
            throw new ConfigurationException("Missing " + KEY_COMPACTION_SCHEDULING_PERIOD + " entry in " + KEY_COMPACTION_SCHEDULING + " entry");
        } else
        {
            configurationBuilder.withCompactionSchedulingPeriod((Integer)compactionSchedulingPeriod);
        }

        // compaction scheduling start now
        Object compactionSchedulingStartNow = compactionScheduling.get(KEY_COMPACTION_SCHEDULING_STARTNOW);
        if (compactionSchedulingStartNow != null) {
            configurationBuilder.withCompactionSchedulingStartNow((Boolean)compactionSchedulingStartNow);
        }

        // solr collection name
        Object dateBucketFormat = compactionScheduling.get(KEY_DATE_BUCKET_FORMAT);
        if (dateBucketFormat == null) {
            throw new ConfigurationException("Missing " + KEY_DATE_BUCKET_FORMAT + " entry in " + KEY_SOLR + " entry");
        } else
        {
            configurationBuilder.withDateBucketFormat((String)dateBucketFormat);
        }

        return configurationBuilder.build();
    }
}
