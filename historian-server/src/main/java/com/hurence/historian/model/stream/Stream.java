package com.hurence.historian.model.stream;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface Stream<E> extends Closeable, Serializable {

    void open() throws IOException;

    E read() throws IOException;

    long getCurrentNumberRead();

    boolean hasNext();
}
