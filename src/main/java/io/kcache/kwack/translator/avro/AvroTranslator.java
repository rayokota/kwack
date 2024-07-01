package io.kcache.kwack.translator.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.ColumnStrategy;
import io.kcache.kwack.schema.DecimalColumnDef;
import io.kcache.kwack.schema.EnumColumnDef;
import io.kcache.kwack.schema.ListColumnDef;
import io.kcache.kwack.schema.MapColumnDef;
import io.kcache.kwack.schema.StructColumnDef;
import io.kcache.kwack.schema.UnionColumnDef;
import io.kcache.kwack.translator.Context;
import io.kcache.kwack.translator.Translator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.duckdb.DuckDBColumnType;

public class AvroTranslator implements Translator {
    @Override
    public ParsedSchema columnDefToSchema(Context ctx, ColumnDef columnDef) {
        return null;

    }

    @Override
    public ColumnDef schemaToColumnDef(Context ctx, ParsedSchema parsedSchema) {
        Schema schema = (Schema) parsedSchema.rawSchema();
        return schemaToColumnDef(ctx, schema);
    }

    private ColumnDef schemaToColumnDef(Context ctx, Schema schema) {
        String logicalType = schema.getProp("logicalType");

        LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        switch (schema.getType()) {
            case RECORD:
                for (Schema.Field field : schema.getFields()) {
                    columnDefs.put(field.name(), schemaToColumnDef(ctx, field.schema()));
                }
                return new StructColumnDef(columnDefs);
            case ENUM:
                return new EnumColumnDef(schema.getEnumSymbols());
            case ARRAY:
                ColumnDef itemDef = schemaToColumnDef(ctx, schema.getElementType());
                return new ListColumnDef(itemDef);
            case MAP:
                ColumnDef valueDef = schemaToColumnDef(ctx, schema.getValueType());
                return new MapColumnDef(new ColumnDef(DuckDBColumnType.VARCHAR), valueDef);
            case UNION:
                int i = 0;
                boolean nullable = false;
                for (Schema subschema : schema.getTypes()) {
                    if (subschema.getType() == Schema.Type.NULL) {
                        nullable = true;
                        continue;
                    }
                    columnDefs.put("u" + i, schemaToColumnDef(ctx, subschema));
                    i++;
                }
                if (columnDefs.size() == 1) {
                    ColumnDef columnDef = columnDefs.values().iterator().next();
                    if (nullable) {
                        columnDef.setColumnStrategy(ColumnStrategy.NULL_STRATEGY);
                    }
                    return columnDef;
                } else {
                    return new UnionColumnDef(columnDefs, nullable
                        ? ColumnStrategy.NULL_STRATEGY
                        : ColumnStrategy.NOT_NULL_STRATEGY);
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
    public Object columnToMessage(
        Context ctx, ColumnDef columnDef, Object column, ParsedSchema parsedSchema) {
        return null;
    }

    @Override
    public Object messageToColumn(
        Context ctx, ParsedSchema parsedSchema, Object message, ColumnDef columnDef) {
        Schema schema = (Schema) parsedSchema.rawSchema();
        return messageToColumn(ctx, schema, message, columnDef);
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
