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

import org.duckdb.DuckDBColumnType;

public class ColumnDef {
    protected final DuckDBColumnType columnType;
    protected ColumnStrategy columnStrategy;

    public ColumnDef(DuckDBColumnType columnType) {
        this(columnType, ColumnStrategy.NOT_NULL_STRATEGY);
    }

    public ColumnDef(DuckDBColumnType columnType, ColumnStrategy columnStrategy) {
        this.columnType = columnType;
        this.columnStrategy = columnStrategy;
    }

    public DuckDBColumnType getColumnType() {
        return columnType;
    }

    public ColumnStrategy getColumnStrategy() {
        return columnStrategy;
    }

    public void setColumnStrategy(ColumnStrategy columnStrategy) {
        this.columnStrategy = columnStrategy;
    }

    public String toDdl() {
        if (columnStrategy != null) {
            // TODO fix default
            return columnType.name() + " " + columnStrategy.toDdl();
        } else {
            return columnType.name();
        }
    }
}
