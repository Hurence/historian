package com.hurence.webapiservice.util;

import java.util.HashSet;
import java.util.Set;


public class StringParserUtils {

    public static Set<String> extractTagsValues(String input) {

        Set<String> tagNames = new HashSet<>();

            for(String kv : input.split("\\|")){
                String[] splits = kv.split("\\$");
                tagNames.add(splits[0]);
            }


        return tagNames;
    }
}
