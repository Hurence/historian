/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.timeseries.iterators;

import java.util.NoSuchElementException;

/**
 * This iterator returns value as often as indicated by number.
 *
 * @param <T> any type
 * @author j.siedersleben
 */
class Repeat<T> implements ImmutableIterator<T> {

    private final T value;
    private final int number;
    private int count = 0;

    /**
     * @param value  to be repeated
     * @param number of repetitions; -1 means indefinitely many
     */
    public Repeat(T value, int number) {
        this.value = value;
        this.number = number;
    }

    /**
     * @return true iff there is a next element
     */
    @Override
    public boolean hasNext() {
        return (number < 0) || (count < number);
    }

    /**
     * @return the next element
     */
    @Override
    public T next() {
        if (number < 0) {
            return value;
        } else if (count < number) {
            count++;
            return value;
        } else {
            throw new NoSuchElementException();
        }
    }
}
