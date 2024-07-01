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

import java.util.LinkedHashMap;
import java.util.Map;
import org.duckdb.DuckDBColumnType;

public class UnionColumnDef extends ColumnDef implements ColumnDefsContainer {
    private final LinkedHashMap<String, ColumnDef> columnDefs;

    public UnionColumnDef(LinkedHashMap<String, ColumnDef> columnDefs) {
        this(ColumnStrategy.NOT_NULL_STRATEGY, columnDefs);
    }

    public UnionColumnDef(
        ColumnStrategy columnStrategy, LinkedHashMap<String, ColumnDef> columnDefs) {
        super(DuckDBColumnType.UNION, columnStrategy);
        this.columnDefs = columnDefs;
    }

    @Override
    public LinkedHashMap<String, ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    @Override
    public String toDdl() {
        StringBuilder sb = new StringBuilder(columnType.name());
        sb.append("(");
        int i = 0;
        for (Map.Entry<String, ColumnDef> entry : columnDefs.entrySet()) {
            String name = entry.getKey();
            ColumnDef columnDef = entry.getValue();
            sb.append(name);
            sb.append(" ");
            sb.append(columnDef.toDdl());
            if (i < columnDefs.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
