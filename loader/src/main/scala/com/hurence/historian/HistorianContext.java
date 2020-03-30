package com.hurence.historian;

import com.hurence.logisland.component.*;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HistorianContext {

    private final Lock lock = new ReentrantLock();

    protected final ConcurrentMap<PropertyDescriptor, String> properties = new ConcurrentHashMap<>();
    private Logger logger = LoggerFactory.getLogger(HistorianContext.class);

    final HistorianProcessor component;

    public HistorianContext(final HistorianProcessor processor) {
        component = processor;
    }


    public PropertyValue getPropertyValue(final PropertyDescriptor descriptor) {
        return getPropertyValue(descriptor.getName());
    }

    public String getProperty(final PropertyDescriptor property) {
        return properties.get(property);
    }

    public ValidationResult setProperty(final String name, final String value) {
        if (null == name || null == value) {
            throw new IllegalArgumentException();
        }

        lock.lock();
        ValidationResult result =null;
        try {



            final PropertyDescriptor descriptor = component.getPropertyDescriptor(name);
            result = descriptor.validate(value);
            if (!result.isValid()) {
                //throw new IllegalArgumentException(result.toString());
                logger.warn(result.toString());
            }

            final String oldValue = properties.put(descriptor, value);

        } catch (final Exception e) {
            // nothing really to do here...
        } finally {
            lock.unlock();
            return result;
        }
    }


    public Map<PropertyDescriptor, String> getProperties() {

        final List<PropertyDescriptor> supported = component.getSupportedPropertyDescriptors();
        if (supported == null || supported.isEmpty()) {
            return Collections.unmodifiableMap(properties);
        } else {
            final Map<PropertyDescriptor, String> props = new LinkedHashMap<>();
            for (final PropertyDescriptor descriptor : supported) {
                props.put(descriptor, null);
            }
            props.putAll(properties);
            return props;
        }

    }

    public PropertyValue getPropertyValue(final String propertyName) {
        final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final String setPropertyValue = getProperty(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return PropertyValueFactory.getInstance(descriptor, propValue, null);
    }


}
