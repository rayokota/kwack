/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kcache.kwack.schema;

import io.kcache.kwack.transformer.Context;

public interface ColumnStrategy {

    ColumnStrategy NOT_NULL_STRATEGY = new NotNullStrategy();

    ColumnStrategy NULL_STRATEGY = new NullStrategy();

    StrategyType getType();

    default Object getDefaultValue() {
        return null;
    }

    enum StrategyType {
        NOT_NULL,
        NULL,
        DEFAULT
    }

    String toDdl(Context ctx);

    class NotNullStrategy implements ColumnStrategy {
        @Override
        public StrategyType getType() {
            return StrategyType.NOT_NULL;
        }

        @Override
        public String toDdl(Context ctx) {
            return "NOT NULL";
        }
    }

    class NullStrategy implements ColumnStrategy {
        @Override
        public StrategyType getType() {
            return StrategyType.NULL;
        }

        @Override
        public String toDdl(Context ctx) {
            return "NULL";
        }
    }

    class DefaultStrategy implements ColumnStrategy {
        private final Object defaultValue;

        public DefaultStrategy(Object defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public StrategyType getType() {
            return StrategyType.DEFAULT;
        }

        @Override
        public Object getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String toDdl(Context ctx) {
            return "DEFAULT " + defaultValue;
        }

    }
}
