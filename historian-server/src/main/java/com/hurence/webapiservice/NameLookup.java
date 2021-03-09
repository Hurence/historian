package com.hurence.webapiservice;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class NameLookup {


    @Data
    static class NameSynonym {
        String name;
        String synonym;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NameLookup.class);

    private static Cache<String, String> lookupTable = Caffeine.newBuilder()
            .maximumSize(50_000)
            .build();

    private static boolean isEnabled = false;

    public static boolean isEnabled() {
        return isEnabled;
    }

    public static void enable() {
        isEnabled = true;
        LOGGER.debug("NameLookup is enabled");
    }

    public static void invalidate() {
        if (isEnabled) {
            lookupTable.invalidateAll();
            LOGGER.debug("cache invalidated");
        } else {
            LOGGER.debug("NameLookup is disabled : cache won't be invalidated");
        }
    }

    public static void loadCacheFromFile(String tagsFilePath, char columnSeparator) throws IOException {
        if (isEnabled) {
            File file = new File(tagsFilePath);
            CsvMapper csvMapper = new CsvMapper();

            // load tags synonyms from file
            MappingIterator<NameSynonym> tags = csvMapper
                    .readerWithSchemaFor(NameSynonym.class)
                    .with(CsvSchema.emptySchema()
                            .withHeader()
                            .withColumnSeparator(columnSeparator))
                    .readValues(file);

            // push them into the cache
            tags.readAll().forEach(nameSynonym -> {
                if (nameSynonym.synonym != null)
                    lookupTable.put(
                            nameSynonym.synonym,
                            nameSynonym.name
                    );
            });

            LOGGER.info("successfully loaded data from file {}", tagsFilePath);
        } else {
            LOGGER.debug("NameLookup is disabled : data not loaded from file {}", tagsFilePath);
        }
    }

    public static Optional<String> getNameFromSynonym(String synonym) {
        String tag = lookupTable.getIfPresent(synonym);
        return tag == null ? Optional.empty() : Optional.of(tag);
    }

    public static void putTag(String tag, String synonym) {
        lookupTable.put(synonym, tag);
    }

}
