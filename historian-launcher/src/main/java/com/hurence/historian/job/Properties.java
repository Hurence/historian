package com.hurence.historian.job;

import java.io.File;
import java.net.URISyntaxException;

public class Properties {

    private Properties() {}

    public static String TMP_DIR = System.getProperty("java.io.tmpdir");
    public static String HISTORIAN_TMP_DIR = TMP_DIR + File.separator + "hurence-historian";
    public static String PIDS_DIR = HISTORIAN_TMP_DIR + File.separator + "pids";
    public static String HISTORIAN_DIR;

    static {
        try {
            HISTORIAN_DIR = new File(Properties.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static String LIB_DIR = HISTORIAN_DIR + File.separator + "lib";
    public static String BIN_DIR = HISTORIAN_DIR + File.separator + "bin";
    public static String CONF_DIR = HISTORIAN_DIR + File.separator + "conf";
    public static String HISTORIAN_SERVER_JAR = LIB_DIR + File.separator + "historian-server-1.3.5-fat.jar";
}
