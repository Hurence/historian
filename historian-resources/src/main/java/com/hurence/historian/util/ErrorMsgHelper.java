package com.hurence.historian.util;

public class ErrorMsgHelper {

    private ErrorMsgHelper() {}

    public static String createMsgError(String prefixMsg, Throwable ex) {
        String error = "";
        if (prefixMsg != null && !prefixMsg.isEmpty()) {
            error = prefixMsg;
            if (ex != null) {
                error+= "\n";
            }
        }
        if (ex != null) {
            error += "Exception type :" + ex.getClass().getCanonicalName() + "\nException message :" + ex.getMessage();
        }
        return error;
    }

    public static String createMsgError(Throwable ex) {
        return createMsgError("", ex);
    }
}
