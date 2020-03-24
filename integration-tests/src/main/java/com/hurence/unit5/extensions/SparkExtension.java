/**
 * Copyright (C) 2019 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.unit5.extensions;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

public class SparkExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkExtension.class);
    public static String CHECKPOINT_DIR_PATH = "/tmp/checkpointDir";

    private static final HashSet<Class> INJECTABLE_TYPES = new HashSet<Class>() {
        {
            add(SparkSession.class);
        }
    };

    private SparkSession sparkSession;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        sparkSession = SparkSession.builder()
                .appName("spark-test")
                .master("local[4]")
                .getOrCreate();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (this.sparkSession != null) this.sparkSession.close();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return INJECTABLE_TYPES.contains(parameterType(parameterContext));
    }

    private Class<?> parameterType(ParameterContext parameterContext) {
        return parameterContext.getParameter().getType();
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterType(parameterContext);
        if (type == SparkSession.class) {
            return sparkSession;
        }
        throw new IllegalStateException("Looks like the ParameterResolver needs a fix...");
    }

    private static Logger getLogger() {
        return LOGGER;
    }
}