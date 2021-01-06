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
package com.hurence.timeseries.metric;


import com.hurence.timeseries.functions.ChronixFunction;

/**
 * The interface defines a Chronix type.
 *
 * @author f.lautenschlager
 */
public interface ChronixType {

    /**
     * @return the type name. Must be unique within an index.
     */
    String getType();

    /**
     * @param function the query name of the function
     * @param args     the arguments that are passed to the function
     * @return the matching function
     */
    ChronixFunction getFunction(String function, String[] args);
}
