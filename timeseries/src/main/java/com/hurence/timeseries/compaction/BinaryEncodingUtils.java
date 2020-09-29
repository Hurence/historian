package com.hurence.timeseries.compaction;

import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;

public class BinaryEncodingUtils {
    public BinaryEncodingUtils() {
    }

    public static String encode(byte[] content) throws UnsupportedEncodingException {
        return new String(Base64.encodeBase64(content), "UTF-8");
    }

    public static byte[] decode(String content) {
        assert content != null : "unable to decode null content";
        return Base64.decodeBase64(content);
    }
}