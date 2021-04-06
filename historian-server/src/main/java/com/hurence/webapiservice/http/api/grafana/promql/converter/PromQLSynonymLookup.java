package com.hurence.webapiservice.http.api.grafana.promql.converter;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.QueryParameter;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * This class
 */
public class PromQLSynonymLookup {


    private static final Logger LOGGER = LoggerFactory.getLogger(PromQLSynonymLookup.class);

    private static final Map<String, String> originalNameSynonymNameMap = new HashMap<>();
    private static final BiMap<String, String> originalSynonymMap = HashBiMap.create();
    private static final Map<String, Map<String, String>> synonymsNamesTagsMap = new HashMap<>();

    private static boolean isEnabled = false;

    public static boolean isEnabled() {
        return isEnabled;
    }

    /**
     * clear the maps and enable the lookup
     */
    public static void enable() {
        originalSynonymMap.clear();
        synonymsNamesTagsMap.clear();
        originalNameSynonymNameMap.clear();
        isEnabled = true;
        LOGGER.debug("NameLookup is enabled");
    }

    @Data
    static class NameSynonym {
        private String name;
        private String synonym;
    }

    /**
     * Load a lookup cache from a CSV file.
     * 
     * the file must contain 2 columns name,synonym
     * 
     * T123.P34_PV.F_CV;Temperature{unit="T123", location="plant_34", type="Mesure_PV"}
     * T123.P34_SP.F_CV;Temperature{unit="T123", location="plant_34", type="Consigne_SP"}
     * T123.P34_OP.F_CV;Temperature{unit="T123", location="plant_34", type="Pour_Cent_OP"}
     *
     * @param tagsFilePath the path of the CSV file
     * @param columnSeparator the separator
     * @throws IOException
     */
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
                if (nameSynonym != null && nameSynonym.name != null && nameSynonym.synonym != null)
                    put(nameSynonym.name, nameSynonym.synonym);
            });

            LOGGER.info("successfully loaded data from file {}", tagsFilePath);
        } else {
            LOGGER.debug("NameLookup is disabled : data not loaded from file {}", tagsFilePath);
        }
    }

    public static QueryParameter getOriginal(QueryParameter synonym) {
        String original = getOriginal(synonym.toQueryString());
        return QueryParameter.builder().parse(original).build();
    }

    public static String getOriginal(String synonym) {
        String reorderdSynonym = QueryParameter.builder().parse(synonym).build().toQueryString();
        String original = originalSynonymMap.inverse().get(reorderdSynonym);
        return original == null ? synonym : original;
    }

    public static String getSynonym(String original) {
        String synonym = originalSynonymMap.get(original);
        return synonym == null ? original : synonym;
    }


    /**
     * Store a new mapping original <=> synonym
     *
     * @param original
     * @param synonym
     */
    public static void put(String original, String synonym) {
        QueryParameter synonymQP = QueryParameter.builder().parse(synonym).build();
        QueryParameter originalQP = QueryParameter.builder().parse(original).build();

        originalSynonymMap.putIfAbsent(originalQP.toQueryString(), synonymQP.toQueryString());
        synonymsNamesTagsMap.putIfAbsent(synonymQP.getName(), new HashMap<>());

        synonymsNamesTagsMap.get(synonymQP.getName()).putAll(synonymQP.getTags().entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));

        originalNameSynonymNameMap.putIfAbsent(originalQP.getName(), synonymQP.getName());
    }

    /**
     * @return all synonyms names
     */
    public static Set<String> getSynonymsNames() {
        return synonymsNamesTagsMap.keySet();
    }

    public static Map<String, String> getSwappedTagsBySynonymName(String name) {
        return synonymsNamesTagsMap.getOrDefault(name, null);
    }

    /**
     * @param original
     * @return the synonym name from an original string
     */
    public static String getSynonymName(String original) {
        String originalName = original;
        if (original.contains("{")) {
            originalName = QueryParameter.builder().parse(original).build().getName();
        }
        return originalNameSynonymNameMap.getOrDefault(originalName, originalName);
    }

    /**
     * @return original name from the synonym
     */
    public static String getOriginalName(String synonym) {
        String original = getOriginal(synonym);
        return QueryParameter.builder().parse(original).build().getName();
    }
}
