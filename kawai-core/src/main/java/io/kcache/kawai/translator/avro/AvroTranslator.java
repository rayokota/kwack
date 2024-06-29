package io.kcache.kawai.translator.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kawai.schema.ColumnDef;
import io.kcache.kawai.schema.ColumnDefsContainer;
import io.kcache.kawai.schema.ColumnStrategy;
import io.kcache.kawai.schema.DecimalColumnDef;
import io.kcache.kawai.schema.EnumColumnDef;
import io.kcache.kawai.schema.ListColumnDef;
import io.kcache.kawai.schema.MapColumnDef;
import io.kcache.kawai.schema.RelDef;
import io.kcache.kawai.schema.StructColumnDef;
import io.kcache.kawai.schema.UnionColumnDef;
import io.kcache.kawai.translator.Context;
import io.kcache.kawai.translator.Translator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.duckdb.DuckDBColumnType;

public class AvroTranslator implements Translator {
    @Override
    public ParsedSchema relDefToSchema(Context ctx, RelDef relDef) {
        return null;

    }

    @Override
    public RelDef schemaToRelDef(Context ctx, ParsedSchema parsedSchema) {
        return schemaToRelDef((Schema) parsedSchema.rawSchema());
    }

    private RelDef schemaToRelDef(Schema schema) {
        ColumnDef columnDef = schemaToColumnDef(schema);
        if (columnDef instanceof ColumnDefsContainer) {
            return new RelDef(((ColumnDefsContainer) columnDef).getColumnDefs());
        } else {
            LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
            columnDefs.put("value", columnDef);
            return new RelDef(columnDefs);
        }
    }

    private ColumnDef schemaToColumnDef(Schema schema) {
        String logicalType = schema.getProp("logicalType");

        LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        switch (schema.getType()) {
            case RECORD:
                for (Schema.Field field : schema.getFields()) {
                    columnDefs.put(field.name(), schemaToColumnDef(field.schema()));
                }
                return new StructColumnDef(columnDefs);
            case ENUM:
                return new EnumColumnDef(schema.getEnumSymbols());
            case ARRAY:
                ColumnDef itemDef = schemaToColumnDef(schema.getElementType());
                return new ListColumnDef(itemDef);
            case MAP:
                ColumnDef valueDef = schemaToColumnDef(schema.getValueType());
                return new MapColumnDef(new ColumnDef(DuckDBColumnType.VARCHAR), valueDef);
            case UNION:
                int i = 0;
                boolean nullable = false;
                for (Schema subschema : schema.getTypes()) {
                    if (subschema.getType() == Schema.Type.NULL) {
                        nullable = true;
                        continue;
                    }
                    columnDefs.put("u" + i, schemaToColumnDef(subschema));
                    i++;
                }
                if (columnDefs.size() == 1) {
                    ColumnDef columnDef = columnDefs.values().iterator().next();
                    if (nullable) {
                        columnDef.setColumnStrategy(ColumnStrategy.NULL_STRATEGY);
                    }
                    return columnDef;
                } else {
                    return new UnionColumnDef(nullable
                        ? ColumnStrategy.NULL_STRATEGY
                        : ColumnStrategy.NOT_NULL_STRATEGY,
                        columnDefs);
                }
            case FIXED:
                return new ColumnDef(DuckDBColumnType.BLOB);
            case STRING:
                if ("uuid".equals(logicalType)) {
                    return new ColumnDef(DuckDBColumnType.UUID);
                }
                return new ColumnDef(DuckDBColumnType.VARCHAR);
            case BYTES:
                if ("decimal".equals(logicalType)) {
                    Object scaleNode = schema.getObjectProp("scale");
                    // In Avro the scale is optional and should default to 0
                    int scale = scaleNode instanceof Number ? ((Number) scaleNode).intValue() : 0;
                    Object precisionNode = schema.getObjectProp("precision");
                    int precision = ((Number) precisionNode).intValue();
                    return new DecimalColumnDef(scale, precision);
                }
                return new ColumnDef(DuckDBColumnType.BLOB);
            case INT:
                if ("date".equals(logicalType)) {
                    return new ColumnDef(DuckDBColumnType.DATE);
                } else if ("time-millis".equals(logicalType)) {
                    // TODO account for no TIME_MS in DuckDB
                    return new ColumnDef(DuckDBColumnType.TIME);
                }
                return new ColumnDef(DuckDBColumnType.INTEGER);
            case LONG:
                if ("time-micros".equals(logicalType)) {
                    return new ColumnDef(DuckDBColumnType.TIME);
                } else if ("timestamp-millis".equals(logicalType)) {
                    return new ColumnDef(DuckDBColumnType.TIMESTAMP_MS);
                } else if ("timestamp-micros".equals(logicalType)) {
                    return new ColumnDef(DuckDBColumnType.TIMESTAMP);
                } else if ("timestamp-nanos".equals(logicalType)) {
                    return new ColumnDef(DuckDBColumnType.TIMESTAMP_NS);
                }
                return new ColumnDef(DuckDBColumnType.BIGINT);
            case FLOAT:
                return new ColumnDef(DuckDBColumnType.FLOAT);
            case DOUBLE:
                return new ColumnDef(DuckDBColumnType.DOUBLE);
            case BOOLEAN:
                return new ColumnDef(DuckDBColumnType.BOOLEAN);
            case NULL:
                // Null type is only supported in unions
                throw new UnsupportedOperationException();
            default:
                break;
        }
        return null;
    }

    @Override
    public Object rowToMessage(Context ctx, RelDef relDef, Object row, ParsedSchema parsedSchema) {
        return null;
    }

    @Override
    public Object messageToRow(
        Context ctx, ParsedSchema parsedSchema, Object message, RelDef relDef) {
        Schema schema = (Schema) parsedSchema.rawSchema();
        if (message instanceof IndexedRecord) {
            IndexedRecord record = (IndexedRecord) message;
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                values.put(field.name(),
                    messageToColumn(ctx, field.schema(), record.get(field.pos()),
                        relDef.getColumnDefs().get(field.name())));
            }
            return values;
        }
        return messageToColumn(ctx, schema, message, relDef.getColumnDefs().get("value"));
    }

    private Object messageToColumn(
        Context ctx, Schema schema, Object message, ColumnDef columnDef) {
        GenericData data;
        switch (schema.getType()) {
            case RECORD:
                StructColumnDef structColumnDef = (StructColumnDef) columnDef;
                data = getData(message);
                Object[] attributes = new Object[schema.getFields().size()];
                int i = 0;
                for (Schema.Field field : schema.getFields()) {
                    ColumnDef fieldColumnDef = structColumnDef.getColumnDefs().get(field.name());
                    Object value = data.getField(message, field.name(), field.pos());
                    if (value instanceof Utf8) {
                        value = value.toString();
                    }
                    Object newValue = messageToColumn(ctx, field.schema(), value, fieldColumnDef);
                    attributes[i++] = newValue;
                }
                return ctx.createStruct(structColumnDef.toDdl(), attributes);
            case ENUM:
                return message != null ? message.toString() : null;
            case ARRAY:
                if (!(message instanceof Iterable)) {
                    return message;
                }
                ListColumnDef listColumnDef = (ListColumnDef) columnDef;
                ColumnDef itemDef = listColumnDef.getItemDef();
                Object[] items = StreamSupport.stream(((Iterable<?>) message).spliterator(), false)
                    .map(it -> messageToColumn(ctx, schema.getElementType(), it, itemDef))
                    .toArray();
                return ctx.createArrayOf(itemDef.toDdl(), items);
            case MAP:
                if (!(message instanceof Map)) {
                    return message;
                }
                MapColumnDef mapColumnDef = (MapColumnDef) columnDef;
                ColumnDef valueDef = mapColumnDef.getValueDef();
                Map<String, Object> map = ((Map<?, ?>) message).entrySet().stream()
                    .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> messageToColumn(ctx, schema.getValueType(), e.getValue(), valueDef),
                        (e1, e2) -> e1));
                return ctx.createMap(valueDef.toDdl(), map);
            case UNION:
                UnionColumnDef unionColumnDef = (UnionColumnDef) columnDef;
                data = getData(message);
                int unionIndex = data.resolveUnion(schema, message);
                return messageToColumn(ctx, schema.getTypes().get(unionIndex), message,
                    unionColumnDef.getColumnDefs().get("u" + unionIndex));
            case FIXED:
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case NULL:
            default:
                break;
        }
        return message;
    }

    private static GenericData getData(Object message) {
        if (message instanceof SpecificRecord) {
            return SpecificData.get();
        } else if (message instanceof GenericRecord) {
            return GenericData.get();
        } else {
            return ReflectData.get();
        }
    }
}
