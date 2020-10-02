package com.hurence.historian.compactor.config;

public class ConfigurationException extends Exception {

    public ConfigurationException(String msg) {
        super(msg);
    }

    public ConfigurationException(String msg, Throwable t) {
        super(msg, t);
    }

    public ConfigurationException(Throwable t) {
        super(t);
    }
}