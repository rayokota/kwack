package io.kcache.kwack.transformer.protobuf;

import static io.kcache.kwack.schema.ColumnStrategy.NULL_STRATEGY;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import io.confluent.protobuf.type.utils.DecimalUtils;
import io.kcache.kwack.transformer.Context;
import io.kcache.kwack.transformer.Transformer;
import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.DecimalColumnDef;
import io.kcache.kwack.schema.ListColumnDef;
import io.kcache.kwack.schema.MapColumnDef;
import io.kcache.kwack.schema.StructColumnDef;
import io.kcache.kwack.schema.UnionColumnDef;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.duckdb.DuckDBColumnType;

public class ProtobufTransformer implements Transformer {
    public static final String MAP_ENTRY_SUFFIX = ProtobufSchema.MAP_ENTRY_SUFFIX;
    public static final String KEY_FIELD = ProtobufSchema.KEY_FIELD;
    public static final String VALUE_FIELD = ProtobufSchema.VALUE_FIELD;

    public static final String PROTOBUF_PRECISION_PROP = "precision";
    public static final String PROTOBUF_SCALE_PROP = "scale";
    public static final String PROTOBUF_DECIMAL_TYPE = "confluent.type.Decimal";
    public static final String PROTOBUF_DATE_TYPE = "google.type.Date";
    public static final String PROTOBUF_TIME_TYPE = "google.type.TimeOfDay";

    public static final String PROTOBUF_TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    public static final String PROTOBUF_DOUBLE_WRAPPER_TYPE = "google.protobuf.DoubleValue";
    public static final String PROTOBUF_FLOAT_WRAPPER_TYPE = "google.protobuf.FloatValue";
    public static final String PROTOBUF_INT64_WRAPPER_TYPE = "google.protobuf.Int64Value";
    public static final String PROTOBUF_UINT64_WRAPPER_TYPE = "google.protobuf.UInt64Value";
    public static final String PROTOBUF_INT32_WRAPPER_TYPE = "google.protobuf.Int32Value";
    public static final String PROTOBUF_UINT32_WRAPPER_TYPE = "google.protobuf.UInt32Value";
    public static final String PROTOBUF_BOOL_WRAPPER_TYPE = "google.protobuf.BoolValue";
    public static final String PROTOBUF_STRING_WRAPPER_TYPE = "google.protobuf.StringValue";
    public static final String PROTOBUF_BYTES_WRAPPER_TYPE = "google.protobuf.BytesValue";

    @Override
    public ColumnDef schemaToColumnDef(Context ctx, ParsedSchema parsedSchema) {
        ProtobufSchema protobufSchema = (ProtobufSchema) parsedSchema;
        Descriptor descriptor = protobufSchema.toDescriptor();
        return schemaToColumnDef(ctx, descriptor);
    }

    private ColumnDef schemaToColumnDef(Context ctx, Descriptor descriptor) {
        ColumnDef columnDef = toUnwrappedColumnDef(descriptor);
        if (columnDef != null) {
            return columnDef;
        }
        LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        StructColumnDef structColumnDef = new StructColumnDef(columnDefs, NULL_STRATEGY);
        ctx.put(descriptor.getFullName(), structColumnDef);

        Set<String> oneOfs = new HashSet<>();
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields().stream()
            .sorted(Comparator.comparing(FieldDescriptor::getIndex))
            .collect(Collectors.toList());
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            OneofDescriptor oneOfDescriptor = fieldDescriptor.getRealContainingOneof();
            if (oneOfDescriptor != null) {
                if (oneOfs.contains(oneOfDescriptor.getName())) {
                    // Already added field as oneof
                    continue;
                }
                columnDefs.put(oneOfDescriptor.getName(), schemaToColumnDef(ctx, oneOfDescriptor));
                oneOfs.add(oneOfDescriptor.getName());
            } else {
                columnDefs.put(fieldDescriptor.getName(), schemaToColumnDef(ctx, fieldDescriptor));
            }
        }
        return structColumnDef;
    }

    private ColumnDef schemaToColumnDef(Context ctx, OneofDescriptor descriptor) {
        LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            columnDefs.put(fieldDescriptor.getName(), schemaToColumnDef(ctx, fieldDescriptor));
        }
        return new UnionColumnDef(columnDefs, NULL_STRATEGY);
    }

    private ColumnDef schemaToColumnDef(Context ctx, FieldDescriptor descriptor) {
        ColumnDef columnDef;
        switch (descriptor.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                columnDef = new ColumnDef(DuckDBColumnType.INTEGER);
                break;
            case UINT32:
            case FIXED32:
                columnDef = new ColumnDef(DuckDBColumnType.UINTEGER);
                break;
            case INT64:
            case SINT64:
            case SFIXED64:
                columnDef = new ColumnDef(DuckDBColumnType.BIGINT);
                break;
            case UINT64:
            case FIXED64:
                columnDef = new ColumnDef(DuckDBColumnType.UBIGINT);
                break;
            case FLOAT:
                columnDef = new ColumnDef(DuckDBColumnType.FLOAT);
                break;
            case DOUBLE:
                columnDef = new ColumnDef(DuckDBColumnType.DOUBLE);
                break;
            case BOOL:
                columnDef = new ColumnDef(DuckDBColumnType.BOOLEAN);
                break;
            case STRING:
                columnDef = new ColumnDef(DuckDBColumnType.VARCHAR);
                break;
            case BYTES:
                columnDef = new ColumnDef(DuckDBColumnType.BLOB);
                break;
            case ENUM:
                // TODO support enum type
                /*
                EnumDescriptor enumDescriptor = descriptor.getEnumType();
                List<String> enumSymbols = enumDescriptor.getValues().stream()
                    .map(EnumValueDescriptor::getName)
                    .collect(Collectors.toList());
                columnDef = new EnumColumnDef(enumSymbols);
                */
                columnDef = new ColumnDef(DuckDBColumnType.VARCHAR);
                break;
            case MESSAGE: {
                String fullName = descriptor.getMessageType().getFullName();
                switch (fullName) {
                    case PROTOBUF_DECIMAL_TYPE:
                        int precision = 0;
                        int scale = 0;
                        if (descriptor.getOptions().hasExtension(MetaProto.fieldMeta)) {
                            Meta fieldMeta = descriptor.getOptions().getExtension(MetaProto.fieldMeta);
                            Map<String, String> params = fieldMeta.getParamsMap();
                            String precisionStr = params.get(PROTOBUF_PRECISION_PROP);
                            if (precisionStr != null) {
                                try {
                                    precision = Integer.parseInt(precisionStr);
                                } catch (NumberFormatException e) {
                                    // ignore
                                }
                            }
                            String scaleStr = params.get(PROTOBUF_SCALE_PROP);
                            if (scaleStr != null) {
                                try {
                                    scale = Integer.parseInt(scaleStr);
                                } catch (NumberFormatException e) {
                                    // ignore
                                }
                            }
                        }
                        columnDef = new DecimalColumnDef(precision, scale);
                        break;
                    case PROTOBUF_DATE_TYPE:
                        columnDef = new ColumnDef(DuckDBColumnType.DATE);
                        break;
                    case PROTOBUF_TIME_TYPE:
                        columnDef = new ColumnDef(DuckDBColumnType.TIME);
                        break;
                    case PROTOBUF_TIMESTAMP_TYPE:
                        columnDef = new ColumnDef(DuckDBColumnType.TIMESTAMP_NS);
                        break;
                    default:
                        columnDef = toUnwrappedOrStructColumnDef(ctx, descriptor);
                        break;
                }
                columnDef.setColumnStrategy(NULL_STRATEGY);
                break;
            }

            default:
                throw new IllegalArgumentException("Unknown schema type: " + descriptor.getType());
        }

        if (descriptor.isRepeated() && columnDef.getColumnType() != DuckDBColumnType.MAP) {
            columnDef = new ListColumnDef(columnDef, NULL_STRATEGY);
        }

        return columnDef;
    }

    private ColumnDef toUnwrappedOrStructColumnDef(
        Context ctx, FieldDescriptor descriptor) {
        ColumnDef columnDef = toUnwrappedColumnDef(descriptor.getMessageType());
        return columnDef != null ? columnDef : toStructColumnDef(ctx, descriptor);
    }

    private ColumnDef toUnwrappedColumnDef(Descriptor descriptor) {
        String fullName = descriptor.getFullName();
        switch (fullName) {
            case PROTOBUF_DOUBLE_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.DOUBLE, NULL_STRATEGY);
            case PROTOBUF_FLOAT_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.FLOAT, NULL_STRATEGY);
            case PROTOBUF_INT64_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.BIGINT, NULL_STRATEGY);
            case PROTOBUF_UINT64_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.UBIGINT, NULL_STRATEGY);
            case PROTOBUF_INT32_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY);
            case PROTOBUF_UINT32_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.UINTEGER, NULL_STRATEGY);
            case PROTOBUF_BOOL_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.BOOLEAN, NULL_STRATEGY);
            case PROTOBUF_STRING_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.VARCHAR, NULL_STRATEGY);
            case PROTOBUF_BYTES_WRAPPER_TYPE:
                return new ColumnDef(DuckDBColumnType.BLOB, NULL_STRATEGY);
            default:
                return null;
        }
    }

    private ColumnDef toStructColumnDef(Context ctx, FieldDescriptor descriptor) {
        if (isMapDescriptor(descriptor)) {
            return toMapColumnDef(ctx, descriptor.getMessageType());
        }
        String fullName = descriptor.getMessageType().getFullName();
        ColumnDef columnDef = ctx.get(fullName);
        if (columnDef != null) {
            return columnDef;
        }
        return schemaToColumnDef(ctx, descriptor.getMessageType());
    }

    private static boolean isMapDescriptor(
        FieldDescriptor fieldDescriptor
    ) {
        if (!fieldDescriptor.isRepeated()) {
            return false;
        }
        Descriptor descriptor = fieldDescriptor.getMessageType();
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        return descriptor.getName().endsWith(MAP_ENTRY_SUFFIX)
            && fieldDescriptors.size() == 2
            && fieldDescriptors.get(0).getName().equals(KEY_FIELD)
            && fieldDescriptors.get(1).getName().equals(VALUE_FIELD)
            && !fieldDescriptors.get(0).isRepeated()
            && !fieldDescriptors.get(1).isRepeated();
    }

    private ColumnDef toMapColumnDef(Context ctx, Descriptor descriptor) {
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        return new MapColumnDef(
            schemaToColumnDef(ctx, fieldDescriptors.get(0)),
            schemaToColumnDef(ctx, fieldDescriptors.get(1))
        );
    }

    @Override
    public Object messageToColumn(
        Context ctx, ParsedSchema parsedSchema, Object message, ColumnDef columnDef) {
        return messageToColumn(ctx, message, columnDef);
    }

    @SuppressWarnings("unchecked")
    private Object messageToColumn(
        Context ctx, Object message, ColumnDef columnDef) {
        if (message == null) {
            return null;
        }
        if (message instanceof List) {
            if (columnDef.getColumnType() == DuckDBColumnType.MAP) {
                MapColumnDef mapColumnDef = (MapColumnDef) columnDef;
                Collection<? extends Message> map = (Collection<? extends Message>) message;
                Map<Object, Object> newMap = new HashMap<>();
                for (Message msg : map) {
                    Descriptor descriptor = msg.getDescriptorForType();
                    Object elemKey = msg.getField(descriptor.findFieldByName(KEY_FIELD));
                    Object elemValue = msg.getField(descriptor.findFieldByName(VALUE_FIELD));
                    newMap.put(
                        messageToColumn(ctx, elemKey, mapColumnDef.getKeyDef()),
                        messageToColumn(ctx, elemValue,  mapColumnDef.getValueDef()));
                }
                return newMap;
            }
            ListColumnDef listColumnDef = (ListColumnDef) columnDef;
            ColumnDef itemDef = listColumnDef.getItemDef();
            return ((List<?>) message).stream()
                .map(it -> messageToColumn(ctx, it, itemDef))
                .collect(Collectors.toList());
        } else if (message instanceof Map) {
            MapColumnDef mapColumnDef = (MapColumnDef) columnDef;
            return ((Map<?, ?>) message).entrySet().stream()
                .collect(Collectors.toMap(
                    e -> messageToColumn(ctx, e.getKey(), mapColumnDef.getKeyDef()),
                    e -> messageToColumn(ctx, e.getValue(), mapColumnDef.getValueDef())
                ));
        } else if (message instanceof Message) {
            Message msg = (Message) message;
            Descriptor descriptor = msg.getDescriptorForType();
            switch (columnDef.getColumnType()) {
                case DECIMAL:
                    return DecimalUtils.toBigDecimal(msg);
                case DATE:
                    return toDate(msg);
                case TIME:
                    return toTime(msg);
                case TIMESTAMP:
                case TIMESTAMP_MS:
                case TIMESTAMP_NS:
                case TIMESTAMP_S:
                    return toTimestamp(msg);
                case UNION:
                    UnionColumnDef unionColumnDef = (UnionColumnDef) columnDef;
                    String unionBranch = descriptor.getName();
                    ctx.putUnionBranch(unionColumnDef, unionBranch);
                    columnDef = unionColumnDef.getColumnDefs().get(unionBranch);
                    // fallthrough
                case STRUCT:
                    StructColumnDef structColumnDef = (StructColumnDef) columnDef;
                    List<Object> attributes = new ArrayList<>();
                    Set<String> oneOfs = new HashSet<>();
                    List<FieldDescriptor> fieldDescriptors = descriptor.getFields().stream()
                        .sorted(Comparator.comparing(FieldDescriptor::getIndex))
                        .collect(Collectors.toList());
                    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
                        OneofDescriptor oneOfDescriptor = fieldDescriptor.getRealContainingOneof();
                        if (oneOfDescriptor != null) {
                            if (oneOfs.contains(oneOfDescriptor.getName())) {
                                // Already added field as oneof
                                continue;
                            }
                            if (msg.hasOneof(oneOfDescriptor)) {
                                UnionColumnDef uColDef = (UnionColumnDef)
                                    structColumnDef.getColumnDefs().get(oneOfDescriptor.getName());
                                FieldDescriptor fd = msg.getOneofFieldDescriptor(oneOfDescriptor);
                                String uBranch = fd.getName();
                                ctx.putUnionBranch(uColDef, uBranch);
                                Object obj = msg.getField(fd);
                                if (obj != null) {
                                    attributes.add(messageToColumn(ctx, obj,
                                        uColDef.getColumnDefs().get(uBranch)));
                                } else {
                                    attributes.add(null);
                                }
                            } else {
                                attributes.add(null);
                            }
                            oneOfs.add(oneOfDescriptor.getName());
                        } else {
                            attributes.add(messageToColumn(ctx, msg.getField(fieldDescriptor),
                                structColumnDef.getColumnDefs().get(fieldDescriptor.getName())));
                        }
                    }
                    return attributes;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + columnDef.getColumnType());
            }
        } else if (message instanceof Enum || message instanceof EnumValueDescriptor) {
            return message.toString();
        } else if (message instanceof ByteString) {
            return ((ByteString) message).toByteArray();
        }
        return message;
    }

    public static LocalDate toDate(Message message) {
        int year = 0;
        int month = 0;
        int day = 0;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getName().equals("year")) {
                year = ((Number) entry.getValue()).intValue();
            } else if (entry.getKey().getName().equals("month")) {
                month = ((Number) entry.getValue()).intValue();
            } else if (entry.getKey().getName().equals("day")) {
                day = ((Number) entry.getValue()).intValue();
            }
        }
        return LocalDate.of(year, month, day);
    }

    public static LocalTime toTime(Message message) {
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int nanos = 0;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getName().equals("hours")) {
                hours = ((Number) entry.getValue()).intValue();
            } else if (entry.getKey().getName().equals("minutes")) {
                minutes = ((Number) entry.getValue()).intValue();
            } else if (entry.getKey().getName().equals("seconds")) {
                seconds = ((Number) entry.getValue()).intValue();
            } else if (entry.getKey().getName().equals("nanos")) {
                nanos = ((Number) entry.getValue()).intValue();
            }
        }
        return LocalTime.of(hours, minutes, seconds, nanos);
    }

    public static Timestamp toTimestamp(Message message) {
        long seconds = 0;
        long nanos = 0;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getName().equals("seconds")) {
                seconds = ((Number) entry.getValue()).longValue();
            } else if (entry.getKey().getName().equals("nanos")) {
                nanos = ((Number) entry.getValue()).longValue();
            }
        }
        return Timestamp.from(Instant.ofEpochSecond(seconds, nanos));
    }
}
