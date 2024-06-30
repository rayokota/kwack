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

import java.util.List;
import org.duckdb.DuckDBColumnType;

public class EnumColumnDef extends ColumnDef {
    private final List<String> enums;

    public EnumColumnDef(List<String> enums) {
        this(ColumnStrategy.NOT_NULL_STRATEGY, enums);
    }

    public EnumColumnDef(ColumnStrategy columnStrategy, List<String> enums) {
        super(DuckDBColumnType.LIST, columnStrategy);
        this.enums = enums;
    }

    public List<String> getEnums() {
        return enums;
    }

    @Override
    public String toDdl() {
        StringBuilder sb = new StringBuilder(columnType.name());
        sb.append(" (");
        for (int i = 0; i < enums.size(); i++) {
            sb.append("'");
            sb.append(enums.get(i));
            sb.append("'");
            if (i < enums.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        String ddl = sb.toString();
        if (columnStrategy != null) {
            // TODO fix default
            return ddl + " " + columnStrategy.getType().name();
        } else {
            return ddl;
        }
    }
}
