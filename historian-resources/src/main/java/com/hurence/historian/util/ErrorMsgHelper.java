package com.hurence.historian.util;

public class ErrorMsgHelper {

    private ErrorMsgHelper() {}

    public static String createMsgError(String prefixMsg, Exception ex) {
        if (prefixMsg != null && !prefixMsg.isEmpty()) {
            return prefixMsg + "\nException type :" + ex.getClass() + "\nException message :" + ex.getMessage();
        }
        return "Exception type :" + ex.getClass() + "\nException message :" + ex.getMessage();
    }

    public static String createMsgError(Exception ex) {
        return createMsgError("", ex);
    }
}
