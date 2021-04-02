package com.hurence.webapiservice;

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

public class QueryParamLookup {


    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParamLookup.class);

    private static Map<String, String> originalNameSynonymNameMap = new HashMap<>();
    private static BiMap<String, String> originalSynonymMap = HashBiMap.create();
    private static Map<String, Map<String, String>> synonymsNamesTagsMap = new HashMap<>();

    private static boolean isEnabled = false;

    public static boolean isEnabled() {
        return isEnabled;
    }

    public static void enable() {
        isEnabled = true;
        originalSynonymMap.clear();
        synonymsNamesTagsMap.clear();
        originalNameSynonymNameMap.clear();
        LOGGER.debug("NameLookup is enabled");
    }

    @Data
    static class NameSynonym {
        private String name;
        private String synonym;
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

    public static QueryParameter getSynonym(QueryParameter original) {
        String synonym = getSynonym(original.toQueryString());
        return QueryParameter.builder().parse(synonym).build();
    }

    public static String getSynonym(String original) {

        String synonym = originalSynonymMap.get(original);
        return synonym == null ? original : synonym;
    }


    public static void put(String original, String synonym) {

        QueryParameter synonymQP = QueryParameter.builder().parse(synonym).build();
        QueryParameter originalQP = QueryParameter.builder().parse(original).build();

        originalSynonymMap.putIfAbsent(originalQP.toQueryString(), synonymQP.toQueryString());

        if (!synonymsNamesTagsMap.containsKey(synonymQP.getName()))
            synonymsNamesTagsMap.put(synonymQP.getName(), new HashMap<>());

        synonymsNamesTagsMap.get(synonymQP.getName()).putAll(synonymQP.getTags().entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));

        originalNameSynonymNameMap.putIfAbsent(originalQP.getName(), synonymQP.getName());
    }

    public static Set<String> getSynonymsNames() {
        return synonymsNamesTagsMap.keySet();
    }

    public static Map<String, String> getSwappedTagsBySynonymName(String name) {
        return synonymsNamesTagsMap.getOrDefault(name, null);
    }

    public static String getSynonymName(String original) {


        String originalName = original;
        if (original.contains("{")) {
            originalName = QueryParameter.builder().parse(original).build().getName();
        }

        return originalNameSynonymNameMap.getOrDefault(originalName, originalName);
    }

    public static String getOriginalName(String synonym) {
        String original = getOriginal(synonym);
        return QueryParameter.builder().parse(original).build().getName();
    }
}
