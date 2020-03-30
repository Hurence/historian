package com.hurence.historian;

import com.hurence.logisland.component.PropertyDescriptor;

import java.io.Serializable;
import java.util.List;

public interface HistorianProcessor extends Serializable {
    PropertyDescriptor getPropertyDescriptor(String name);

    List<PropertyDescriptor> getSupportedPropertyDescriptors();

}
