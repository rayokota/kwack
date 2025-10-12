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
import java.util.Objects;
import org.duckdb.DuckDBColumnType;

public class MapColumnDef extends ColumnDef {
    private final ColumnDef keyDef;
    private final ColumnDef valueDef;

    public MapColumnDef(ColumnDef keyDef, ColumnDef valueDef) {
        this(keyDef, valueDef, ColumnStrategy.NOT_NULL_STRATEGY);
    }

    public MapColumnDef(ColumnDef keyDef, ColumnDef valueDef, ColumnStrategy columnStrategy) {
        super(DuckDBColumnType.MAP, columnStrategy);
        this.keyDef = keyDef;
        this.valueDef = valueDef;
    }

    public ColumnDef getKeyDef() {
        return keyDef;
    }

    public ColumnDef getValueDef() {
        return valueDef;
    }

    @Override
    public String toDdl(Context ctx) {
        return columnType.name() + "(" + keyDef.toDdl(ctx) + ", " + valueDef.toDdl(ctx) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MapColumnDef that = (MapColumnDef) o;
        return Objects.equals(keyDef, that.keyDef)
            && Objects.equals(valueDef, that.valueDef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyDef, valueDef);
    }
}
