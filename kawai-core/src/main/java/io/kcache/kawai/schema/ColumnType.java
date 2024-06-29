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
package io.kcache.kawai.schema;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public enum ColumnType {
    BOOLEAN(boolean.class, "boolean"),
    INT(int.class, "int"),
    LONG(long.class, "long"),
    FLOAT(float.class, "float"),
    DOUBLE(double.class, "double"),
    BYTES(byte[].class, "bytes"),
    STRING(String.class, "string"),
    DECIMAL(BigDecimal.class, "decimal"),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private final Class clazz;
    private final String simpleName;

    private static final Map<String, ColumnType> NAMES = new HashMap<>();

    static {
        for (ColumnType value : values()) {
            NAMES.put(value.simpleName, value);
        }
        // Handle both int and Integer
        NAMES.put(Integer.class.getSimpleName(), INT);
    }

    ColumnType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    // See org.apache.calcite.sql.type.JavaToSqlTypeConversionRules
    public Class<?> getType() {
        return clazz;
    }

    public static ColumnType of(String typeString) {
        return NAMES.get(typeString);
    }
}
