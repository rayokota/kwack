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

public class DecimalColumnDef extends ColumnDef {
    private final int precision;
    private final int scale;

    public DecimalColumnDef(int precision, int scale) {
        this(precision, scale, ColumnStrategy.NOT_NULL_STRATEGY);
    }

    public DecimalColumnDef(int precision, int scale, ColumnStrategy columnStrategy) {
        super(DuckDBColumnType.DECIMAL, columnStrategy);
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public String toDdl(Context ctx) {
        return columnType.name() + "(" + precision + ", " + scale + ")";
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
        DecimalColumnDef that = (DecimalColumnDef) o;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }
}
