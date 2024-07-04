package io.kcache.kwack.transformer.avro;

import static io.kcache.kwack.schema.ColumnStrategy.NOT_NULL_STRATEGY;
import static io.kcache.kwack.schema.ColumnStrategy.NULL_STRATEGY;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.DecimalColumnDef;
import io.kcache.kwack.schema.EnumColumnDef;
import io.kcache.kwack.schema.ListColumnDef;
import io.kcache.kwack.schema.MapColumnDef;
import io.kcache.kwack.schema.StructColumnDef;
import io.kcache.kwack.schema.UnionColumnDef;
import io.kcache.kwack.transformer.Context;
import io.kcache.kwack.transformer.Transformer;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.duckdb.DuckDBColumnType;

public class AvroTransformer implements Transformer {
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
                StructColumnDef structColumnDef = new StructColumnDef(columnDefs);
                for (Schema.Field field : schema.getFields()) {
                    columnDefs.put(field.name(), schemaToColumnDef(ctx, field.schema()));
                }
                return structColumnDef;
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
                for (Schema subSchema : schema.getTypes()) {
                    if (subSchema.getType() == Schema.Type.NULL) {
                        nullable = true;
                        continue;
                    }
                    columnDefs.put("u" + i, schemaToColumnDef(ctx, subSchema));
                    i++;
                }
                if (columnDefs.size() == 1) {
                    ColumnDef columnDef = columnDefs.values().iterator().next();
                    if (nullable) {
                        columnDef.setColumnStrategy(NULL_STRATEGY);
                    }
                    return columnDef;
                } else {
                    return new UnionColumnDef(columnDefs, nullable
                        ? NULL_STRATEGY
                        : NOT_NULL_STRATEGY);
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
                return new ColumnDef(DuckDBColumnType.BLOB, NULL_STRATEGY);
            default:
                break;
        }
        throw new IllegalArgumentException();
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
                if (columnDef instanceof UnionColumnDef) {
                    UnionColumnDef unionColumnDef = (UnionColumnDef) columnDef;
                    data = getData(message);
                    int unionIndex = data.resolveUnion(schema, message);
                    String unionBranch = "u" + unionIndex;
                    ctx.putUnionBranch(unionColumnDef, unionBranch);
                    return messageToColumn(ctx, schema.getTypes().get(unionIndex), message,
                        unionColumnDef.getColumnDefs().get(unionBranch));
                }
                for (Schema subSchema : schema.getTypes()) {
                    if (subSchema.getType() != Schema.Type.NULL) {
                        return messageToColumn(ctx, subSchema, message, columnDef);
                    }
                }
                break;
            case FIXED:
            case BYTES:
                if (message instanceof ByteBuffer) {
                    message = ((ByteBuffer) message).array();
                } else if (message instanceof GenericFixed) {
                    message = ((GenericFixed) message).bytes();
                }
                break;
            case STRING:
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
