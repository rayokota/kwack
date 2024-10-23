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
package io.kcache.kwack;

import static io.kcache.kwack.schema.ColumnStrategy.NULL_STRATEGY;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.caffeine.CaffeineCache;
import io.kcache.kwack.KwackConfig.RowAttribute;
import io.kcache.kwack.KwackConfig.Serde;
import io.kcache.kwack.KwackConfig.SerdeType;
import io.kcache.kwack.schema.UnionColumnDef;
import io.kcache.kwack.sqlline.KwackApplication;
import io.kcache.kwack.transformer.Transformer;
import io.kcache.kwack.transformer.json.JsonTransformer;
import io.kcache.kwack.transformer.protobuf.ProtobufTransformer;
import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.MapColumnDef;
import io.kcache.kwack.schema.StructColumnDef;
import io.kcache.kwack.transformer.Context;
import io.kcache.kwack.transformer.avro.AvroTransformer;
import io.reactivex.rxjava3.core.Observable;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import java.io.ByteArrayOutputStream;
import java.io.UncheckedIOException;
import java.sql.Blob;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.IntStream;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import sqlline.SqlLine;
import sqlline.SqlLine.Status;

public class KwackEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KwackEngine.class);

    public static final String MOCK_SR = "mock://kwack";
    public static final String ROWKEY = "rowkey";
    public static final String ROWVAL = "rowval";
    public static final String ROWINFO = "rowinfo";

    private static final int MAGIC_BYTE = 0x0;

    private KwackConfig config;
    private DuckDBConnection conn;
    private SchemaRegistryClient schemaRegistry;
    private SchemaRegistryClient mockSchemaRegistry;
    private Map<String, SchemaProvider> schemaProviders;
    private Map<String, Serde> keySerdes;
    private Map<String, Serde> valueSerdes;
    private final Map<String, ColumnDef> keyColDefs = new HashMap<>();
    private final Map<String, ColumnDef> valueColDefs = new HashMap<>();
    private final Map<Tuple3<Boolean, Serde, ParsedSchema>, Deserializer<?>> deserializers = new HashMap<>();
    private final Map<ParsedSchema, ColumnDef> columnDefs = new HashMap<>();
    private String query;
    private EnumSet<RowAttribute> rowAttributes;
    private int rowInfoSize;
    private final Map<String, Tuple2<Serde, ParsedSchema>> keySchemas = new HashMap<>();
    private final Map<String, Tuple2<Serde, ParsedSchema>> valueSchemas = new HashMap<>();
    private final Map<String, KafkaCache<Bytes, Bytes>> caches;
    private final AtomicBoolean initialized;

    private static KwackEngine INSTANCE;

    public synchronized static KwackEngine getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new KwackEngine();
        }
        return INSTANCE;
    }

    public synchronized static void closeInstance() {
        if (INSTANCE != null) {
            try {
                INSTANCE.close();
            } catch (IOException e) {
                LOG.warn("Could not close engine", e);
            }
            INSTANCE = null;
        }
    }

    private KwackEngine() {
        caches = new HashMap<>();
        initialized = new AtomicBoolean();
        // Use UTC for all times
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    public boolean hasRowAttribute(RowAttribute attribute) {
        return !rowAttributes.contains(RowAttribute.NONE) && rowAttributes.contains(attribute);
    }

    public void configure(Map<String, ?> configs) {
        configure(new KwackConfig(configs));
    }

    public void configure(KwackConfig config) {
        this.config = config;
    }

    public void init() {
        try {
            conn = (DuckDBConnection) DriverManager.getConnection(config.getDbUrl());

            List<SchemaProvider> providers = Arrays.asList(
                new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
            );
            schemaRegistry = createSchemaRegistry(
                config.getSchemaRegistryUrls(), providers, config.originals());
            mockSchemaRegistry = createSchemaRegistry(
                Collections.singletonList(MOCK_SR), providers, config.originals());

            schemaProviders = providers.stream()
                .collect(Collectors.toMap(SchemaProvider::schemaType, p -> p));

            keySerdes = config.getKeySerdes();
            valueSerdes = config.getValueSerdes();

            query = config.getQuery();
            rowAttributes = config.getRowAttributes();
            rowInfoSize = getRowInfoSize();

            initTables(conn);
            initCaches(conn);

            boolean isInitialized = initialized.compareAndSet(false, true);
            if (!isInitialized) {
                throw new IllegalStateException("Illegal state while initializing engine. Engine "
                    + "was already initialized");
            }
        } catch (SQLException e) {
            LOG.error("Could not initialize engine", e);
            throw new RuntimeException("Could not initialize engine", e);
        }
    }

    public Observable<Map<String, Object>> start() throws IOException {
        if (query != null && !query.isEmpty()) {
            return Observable.using(
                () -> conn.createStatement(),

                stmt -> Observable.create(subscriber -> {
                    try (ResultSet rs = stmt.executeQuery(query)) {
                        ResultSetMetaData md = rs.getMetaData();
                        int numCols = md.getColumnCount();
                        List<String> colNames = IntStream.range(0, numCols)
                            .mapToObj(i -> {
                                try {
                                    return md.getColumnName(i + 1);
                                } catch (SQLException e) {
                                    return "?";
                                }
                            })
                            .collect(Collectors.toList());
                        while (!subscriber.isDisposed() && rs.next()) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            for (int i = 0; i < numCols; i++) {
                                String name = colNames.get(i);
                                row.put(name, toJson(rs.getObject(i + 1)));
                            }
                            subscriber.onNext(row);
                        }
                        subscriber.onComplete();
                    }
                }),

                Statement::close
            );
        } else {
            start(new String[]{
                "-ac", KwackApplication.class.getName(), "-u", config.getDbUrl()}, true);
            return Observable.empty();
        }
    }

    public Status start(String[] args, boolean saveHistory) throws IOException {
        SqlLine sqlline = new SqlLine();
        Status status = sqlline.begin(args, null, saveHistory);
        if (!Boolean.getBoolean("sqlline.system.exit")) {
            System.exit(status.ordinal());
        }
        return status;
    }

    @SuppressWarnings("unchecked")
    private static Object toJson(Object obj) {
        try {
            if (obj instanceof DuckDBStruct) {
                return toJson(((DuckDBStruct) obj).getMap());
            } else if (obj instanceof DuckDBArray) {
                return toJson(((DuckDBArray) obj).getArray());
            } else if (obj instanceof Map) {
                Map<String, Object> m = new LinkedHashMap<>((Map<String, Object>) obj);
                for (Map.Entry<String, Object> entry : m.entrySet()) {
                    entry.setValue(toJson(entry.getValue()));
                }
                return m;
            } else if (obj instanceof Object[]) {
                return Arrays.stream((Object[]) obj)
                    .map(KwackEngine::toJson)
                    .collect(Collectors.toList());
            } else if (obj instanceof Blob) {
                byte[] bytes = ((Blob) obj).getBinaryStream().readAllBytes();
                return Base64.getEncoder().encodeToString(bytes);
            } else {
                return obj;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static SchemaRegistryClient createSchemaRegistry(
        List<String> urls, List<SchemaProvider> providers, Map<String, Object> configs) {
        if (urls == null || urls.isEmpty()) {
            return null;
        }
        String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
            return MockSchemaRegistry.getClientForScope(mockScope, providers);
        } else {
            return new CachedSchemaRegistryClient(urls, 1000, providers, configs);
        }
    }

    public static void resetSchemaRegistry(List<String> urls, SchemaRegistryClient schemaRegistry) {
        if (urls != null && !urls.isEmpty()) {
            String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(urls);
            if (mockScope != null) {
                MockSchemaRegistry.dropScope(mockScope);
            } else {
                schemaRegistry.reset();
            }
        }
    }

    public SchemaRegistryClient getSchemaRegistry() {
        if (schemaRegistry == null) {
            throw new ConfigException("Missing schema registry URL");
        }
        return schemaRegistry;
    }

    public SchemaRegistryClient getMockSchemaRegistry() {
        return mockSchemaRegistry;
    }

    public SchemaProvider getSchemaProvider(String schemaType) {
        return schemaProviders.get(schemaType);
    }

    public Tuple2<Serde, ParsedSchema> getKeySchema(Serde serde, String topic ) {
        return keySchemas.computeIfAbsent(topic, t -> getSchema(topic + "-key", serde));
    }

    public Tuple2<Serde, ParsedSchema> getValueSchema(Serde serde, String topic) {
        return valueSchemas.computeIfAbsent(topic, t -> getSchema(topic + "-value", serde));
    }

    private Tuple2<Serde, ParsedSchema> getSchema(String subject, Serde serde) {
        SerdeType serdeType = serde.getSerdeType();
        switch (serdeType) {
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BINARY:
                return Tuple.of(serde, null);
            case JSON:
                return serde.getSchema() == null
                    ? Tuple.of(serde, null)
                    : parseSchema(serde).map(s -> createTuple(serde, s))
                        .orElseGet(() -> Tuple.of(new Serde(SerdeType.BINARY), null));
            case AVRO:
            case PROTO:
                return parseSchema(serde).map(s -> createTuple(serde, s))
                    .orElseGet(() -> Tuple.of(new Serde(SerdeType.BINARY), null));
            case LATEST:
                return getLatestSchema(subject).map(s -> createTuple(serde, s))
                    .orElseGet(() -> Tuple.of(new Serde(SerdeType.BINARY), null));
            case ID:
                return getSchemaById(serde.getId()).map(s -> createTuple(serde, s))
                    .orElseGet(() -> Tuple.of(new Serde(SerdeType.BINARY), null));
            default:
                throw new IllegalArgumentException("Illegal serde type: " + serde.getSerdeType());
        }
    }

    private Tuple2<Serde, ParsedSchema> createTuple(Serde serde, ParsedSchema schema) {
        if (serde.getMessage() != null
            && !serde.getMessage().isEmpty()
            && schema instanceof ProtobufSchema) {
            ProtobufSchema protobufSchema = (ProtobufSchema) schema;
            schema = protobufSchema.copy(serde.getMessage());
        }
        return Tuple.of(serde, schema);
    }

    public Optional<ParsedSchema> parseSchema(Serde serde) {
        String schemaType = serde.getSchemaType();
        String schema = serde.getSchema();
        try {
            Schema s = new Schema(null, null, null, schemaType, null, schema);
            ParsedSchema parsedSchema =
                getSchemaProvider(schemaType).parseSchemaOrElseThrow(s, false, false);
            parsedSchema.validate(false);
            int id = getMockSchemaRegistry().register(schema, parsedSchema);
            serde.setId(id);
            return Optional.of(parsedSchema);
        } catch (Exception e) {
            LOG.error("Could not parse schema {}", schema, e);
            return Optional.empty();
        }
    }

    public Optional<ParsedSchema> getLatestSchema(String subject) {
        if (subject == null) {
            return Optional.empty();
        }
        try {
            SchemaMetadata schema = getSchemaRegistry().getLatestSchemaMetadata(subject);
            return getSchemaRegistry().parseSchema(new Schema(null, schema));
        } catch (Exception e) {
            LOG.error("Could not find latest schema for subject {}", subject, e);
            return Optional.empty();
        }
    }

    public Optional<ParsedSchema> getSchemaById(int id) {
        try {
            ParsedSchema schema = getSchemaRegistry().getSchemaById(id);
            return Optional.of(schema);
        } catch (Exception e) {
            LOG.error("Could not find schema with id {}", id, e);
            return Optional.empty();
        }
    }

    public Tuple2<Context, Object> deserializeKey(String topic, byte[] bytes) throws IOException {
        return deserialize(true, null, topic, bytes);
    }

    public Tuple2<Context, Object> deserializeValue(String topic, Object key, byte[] bytes) throws IOException {
        return deserialize(false, key, topic, bytes);
    }

    private Tuple2<Context, Object> deserialize(boolean isKey, Object key, String topic, byte[] bytes) throws IOException {
        if (bytes == null || bytes == Bytes.EMPTY) {
            return Tuple.of(null, null);
        }

        Serde serde = isKey
            ? keySerdes.getOrDefault(topic, Serde.KEY_DEFAULT)
            : valueSerdes.getOrDefault(topic, Serde.VALUE_DEFAULT);

        Tuple2<Serde, ParsedSchema> schema =
            isKey ? getKeySchema(serde, topic) : getValueSchema(serde, topic);

        Deserializer<?> deserializer = getDeserializer(isKey, schema);

        if (serde.usesExternalSchema() || config.getSkipBytes() > 0) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                if (serde.usesExternalSchema()) {
                    out.write(MAGIC_BYTE);
                    out.write(ByteBuffer.allocate(4).putInt(serde.getId()).array());
                    if (serde.getSerdeType() == SerdeType.PROTO) {
                        MessageIndexes indexes;
                        if (serde.getMessage() != null && !serde.getMessage().isEmpty()) {
                            ProtobufSchema protobufSchema = (ProtobufSchema) schema._2;
                            indexes = protobufSchema.toMessageIndexes(serde.getMessage());
                        } else {
                            // assume message type is first in schema
                            indexes = new MessageIndexes(Collections.singletonList(0));
                        }
                        out.write(indexes.toByteArray());
                    }
                }
                int skipBytes = config.getSkipBytes();
                if (skipBytes < bytes.length) {
                    out.write(bytes, skipBytes, bytes.length - skipBytes);
                }
                bytes = out.toByteArray();
            }
        }

        Context ctx = new Context(isKey, conn);
        Object object = deserializer.deserialize(topic, bytes);
        ctx.setOriginalMessage(object);
        if (object != null && schema._2 != null) {
            ParsedSchema parsedSchema = schema._2;
            Transformer transformer;
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    transformer = new AvroTransformer();
                    break;
                case "JSON":
                    transformer = new JsonTransformer();
                    if (!isKey && serde.getTag() != null
                        && key instanceof ObjectNode
                        && object instanceof ObjectNode) {
                        ObjectNode keyNode = (ObjectNode) key;
                        ObjectNode valueNode = (ObjectNode) object;
                        valueNode.set(
                            serde.getTag().getTarget(),
                            keyNode.get(serde.getTag().getSource()));
                    }
                    break;
                case "PROTOBUF":
                    transformer = new ProtobufTransformer();
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
            ColumnDef columnDef = schemaToColumnDef(ctx, transformer, parsedSchema);
            object = transformer.messageToColumn(ctx, parsedSchema, object, columnDef);
        } else if (object instanceof Bytes) {
            object = ((Bytes) object).get();
        }

        return Tuple.of(ctx, object);
    }

    public Deserializer<?> getDeserializer(boolean isKey, Tuple2<Serde, ParsedSchema> schema) {
        return deserializers.computeIfAbsent(Tuple.of(isKey, schema._1, schema._2),
            k -> createDeserializer(isKey, schema));
    }

    private Deserializer<?> createDeserializer(boolean isKey, Tuple2<Serde, ParsedSchema> schema) {
        if (schema._2 != null) {
            ParsedSchema parsedSchema = schema._2;
            SchemaRegistryClient schemaRegistry = null;
            Map<String, Object> originals = new HashMap<>(config.originals());
            switch (schema._1.getSerdeType()) {
                case LATEST:
                    schemaRegistry = getSchemaRegistry();
                    originals.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
                    break;
                case ID:
                    schemaRegistry = getSchemaRegistry();
                    originals.put(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, schema._1.getId());
                    break;
                case AVRO:
                case JSON:
                case PROTO:
                    schemaRegistry = getMockSchemaRegistry();
                    originals.put(AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID, schema._1.getId());
                    break;
            }
            Deserializer<?> deserializer = null;
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    // This allows BigDecimal to be passed through unchanged
                    originals.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
                    deserializer = new KafkaAvroDeserializer(schemaRegistry);
                    break;
                case "JSON":
                    // Set the type to null so JsonNode is produced
                    // Otherwise the type defaults to Object.class which produces a LinkedHashMap
                    originals.put(KafkaJsonDeserializerConfig.JSON_KEY_TYPE, null);
                    originals.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, null);
                    deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistry);
                    break;
                case "PROTOBUF":
                    deserializer = new KafkaProtobufDeserializer<>(schemaRegistry);
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
            deserializer.configure(originals, isKey);
            return deserializer;
        } else {
            switch (schema._1.getSerdeType()) {
                case STRING:
                case JSON:
                    return new StringDeserializer();
                case SHORT:
                    return new ShortDeserializer();
                case INT:
                    return new IntegerDeserializer();
                case LONG:
                    return new LongDeserializer();
                case FLOAT:
                    return new FloatDeserializer();
                case DOUBLE:
                    return new DoubleDeserializer();
                case BINARY:
                    return new BytesDeserializer();
                default:
                    throw new IllegalArgumentException("Illegal type " + schema._1);
            }
        }
    }

    private void initTables(DuckDBConnection conn) {
        for (String topic : config.getTopics()) {
            initTable(conn, topic);
        }
    }

    private void initTable(DuckDBConnection conn, String topic) {
        Serde keySerde = keySerdes.getOrDefault(topic, Serde.KEY_DEFAULT);
        Serde valueSerde = valueSerdes.getOrDefault(topic, Serde.VALUE_DEFAULT);

        Tuple2<Serde, ParsedSchema> keySchema = getKeySchema(keySerde, topic);
        Tuple2<Serde, ParsedSchema> valueSchema = getValueSchema(valueSerde, topic);

        ColumnDef keyColDef = keyColDefs.computeIfAbsent(
            topic, k -> toColumnDef(true, keySchema));
        ColumnDef valueColDef = valueColDefs.computeIfAbsent(
            topic, k -> toColumnDef(false, valueSchema));

        String valueDdl;
        if (valueColDef.getColumnType() == DuckDBColumnType.STRUCT) {
            StructColumnDef structColDef = (StructColumnDef) valueColDef;
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, ColumnDef> entry : structColDef.getColumnDefs().entrySet()) {
                sb.append("\"");
                sb.append(entry.getKey());
                sb.append("\" ");
                sb.append(entry.getValue().toDdlWithStrategy());
                sb.append(", ");
            }
            valueDdl = sb.toString();
        } else {
            valueDdl = ROWVAL + " " + valueColDef.toDdlWithStrategy() + ", ";
        }

        String ddl;
        StructColumnDef rowInfoDef = getRowInfoDef();
        if (rowInfoSize > 0) {
            ddl = "CREATE TYPE rowinfo AS " + rowInfoDef.toDdl();
            try {
                conn.createStatement().execute(ddl);
            } catch (SQLException e) {
                // ignore, as type may already exist if more than one topic is being processed
            }
        }

        ddl = "CREATE TABLE IF NOT EXISTS \"" + topic + "\" (";
        if (hasRowAttribute(RowAttribute.ROWKEY)) {
            ddl += ROWKEY + " " + keyColDef.toDdlWithStrategy() + ", ";
        }
        ddl += valueDdl;
        if (rowInfoSize > 0) {
            ddl += ROWINFO + " " + ROWINFO;
        }
        ddl += ")";
        try {
            conn.createStatement().execute(ddl);
        } catch (SQLException e) {
            LOG.error("Could not execute DDL: {}", ddl, e);
            throw new RuntimeException("Could not execute DDL: " + ddl, e);
        }
    }

    private ColumnDef toColumnDef(boolean isKey, Tuple2<Serde, ParsedSchema> schema) {
        if (schema._2 != null) {
            Transformer transformer;
            ParsedSchema parsedSchema = schema._2;
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    transformer = new AvroTransformer();
                    break;
                case "JSON":
                    transformer = new JsonTransformer();
                    break;
                case "PROTOBUF":
                    transformer = new ProtobufTransformer();
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
            return schemaToColumnDef(new Context(isKey, conn), transformer, parsedSchema);
        }
        switch (schema._1.getSerdeType()) {
            case STRING:
                return new ColumnDef(DuckDBColumnType.VARCHAR, NULL_STRATEGY);
            case JSON:
                return new ColumnDef(DuckDBColumnType.JSON, NULL_STRATEGY);
            case SHORT:
                return new ColumnDef(DuckDBColumnType.SMALLINT, NULL_STRATEGY);
            case INT:
                return new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY);
            case LONG:
                return new ColumnDef(DuckDBColumnType.BIGINT, NULL_STRATEGY);
            case FLOAT:
                return new ColumnDef(DuckDBColumnType.FLOAT, NULL_STRATEGY);
            case DOUBLE:
                return new ColumnDef(DuckDBColumnType.DOUBLE, NULL_STRATEGY);
            case BINARY:
                return new ColumnDef(DuckDBColumnType.BLOB, NULL_STRATEGY);
            default:
                throw new IllegalArgumentException("Illegal type " + schema._1);
        }
    }

    private ColumnDef schemaToColumnDef(
        Context ctx, Transformer transformer, ParsedSchema parsedSchema) {
        return columnDefs.computeIfAbsent(parsedSchema, s -> transformer.schemaToColumnDef(ctx, s));
    }

    private int getRowInfoSize() {
        EnumSet<RowAttribute> copy = EnumSet.copyOf(rowAttributes);
        copy.remove(RowAttribute.ROWKEY);
        copy.remove(RowAttribute.NONE);
        return copy.size();
    }

    private StructColumnDef getRowInfoDef() {
        LinkedHashMap<String, ColumnDef> defs = new LinkedHashMap<>();
        if (hasRowAttribute(RowAttribute.KSI)) {
            defs.put(RowAttribute.KSI.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY));
        }
        if (hasRowAttribute(RowAttribute.VSI)) {
            defs.put(RowAttribute.VSI.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY));
        }
        if (hasRowAttribute(RowAttribute.TOP)) {
            defs.put(RowAttribute.TOP.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.VARCHAR));
        }
        if (hasRowAttribute(RowAttribute.PAR)) {
            defs.put(RowAttribute.PAR.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER));
        }
        if (hasRowAttribute(RowAttribute.OFF)) {
            defs.put(RowAttribute.OFF.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.BIGINT));
        }
        if (hasRowAttribute(RowAttribute.TS)) {
            defs.put(RowAttribute.TS.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.BIGINT));
        }
        if (hasRowAttribute(RowAttribute.TST)) {
            defs.put(RowAttribute.TST.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.SMALLINT));
        }
        if (hasRowAttribute(RowAttribute.EPO)) {
            defs.put(RowAttribute.EPO.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY));
        }
        if (hasRowAttribute(RowAttribute.HDR)) {
            defs.put(RowAttribute.HDR.name().toLowerCase(Locale.ROOT), new MapColumnDef(
                new ColumnDef(DuckDBColumnType.VARCHAR),
                new ColumnDef(DuckDBColumnType.VARCHAR)));
        }
        return new StructColumnDef(defs);
    }

    private void initCaches(DuckDBConnection conn) {
        for (String topic : config.getTopics()) {
            initCache(conn, topic);
        }
    }

    private void initCache(DuckDBConnection conn, String topic) {
        Map<String, Object> originals = config.originals();
        Map<String, Object> configs = new HashMap<>(originals);
        for (Map.Entry<String, Object> config : originals.entrySet()) {
            if (!config.getKey().startsWith("kafkacache.")) {
                configs.put("kafkacache." + config.getKey(), config.getValue());
            }
        }
        String groupId = (String)
            configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kwack-1");
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        KafkaCache<Bytes, Bytes> cache = new KafkaCache<>(
            new KafkaCacheConfig(configs),
            Serdes.Bytes(),
            Serdes.Bytes(),
            new UpdateHandler(conn),
            new CaffeineCache<>(100, Duration.ofMillis(10000), null)
        );
        cache.init();
        caches.put(topic, cache);
    }

    class UpdateHandler implements CacheUpdateHandler<Bytes, Bytes> {
        private final DuckDBConnection conn;
        private final Map<String, PreparedStatement> stmts = new HashMap<>();

        public UpdateHandler(DuckDBConnection conn) {
            this.conn = conn;
        }

        @Override
        public void handleUpdate(Headers headers,
                                 Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts, TimestampType tsType,
                                 Optional<Integer> leaderEpoch) {
            String topic = tp.topic();
            Integer keySchemaId = null;
            Integer valueSchemaId = null;
            Tuple2<Context, Object> keyObj = null;
            Tuple2<Context, Object> valueObj = null;

            List<String> paramMarkers = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            String sql = null;
            try {
                Serde keySerde = keySerdes.getOrDefault(topic, Serde.KEY_DEFAULT);
                if (keySerde.usesSchemaRegistry()) {
                    keySchemaId = schemaIdFor(key.get());
                }
                keyObj = deserializeKey(topic, key != null ? key.get() : null);

                Serde valueSerde = valueSerdes.getOrDefault(topic, Serde.VALUE_DEFAULT);
                if (valueSerde.usesSchemaRegistry()) {
                    valueSchemaId = schemaIdFor(value.get());
                }
                Object originalKey = keyObj._1 != null ? keyObj._1.getOriginalMessage() : null;
                valueObj = deserializeValue(
                    topic, originalKey, value != null ? value.get() : null);

                Struct rowInfo = null;
                if (rowInfoSize > 0) {
                    Object[] rowAttrs = new Object[rowInfoSize];
                    int index = 0;
                    if (hasRowAttribute(RowAttribute.KSI)) {
                        rowAttrs[index++] = keySchemaId;
                    }
                    if (hasRowAttribute(RowAttribute.VSI)) {
                        rowAttrs[index++] = valueSchemaId;
                    }
                    if (hasRowAttribute(RowAttribute.TOP)) {
                        rowAttrs[index++] = topic;
                    }
                    if (hasRowAttribute(RowAttribute.PAR)) {
                        rowAttrs[index++] = tp.partition();
                    }
                    if (hasRowAttribute(RowAttribute.OFF)) {
                        rowAttrs[index++] = offset;
                    }
                    if (hasRowAttribute(RowAttribute.TS)) {
                        rowAttrs[index++] = ts;
                    }
                    if (hasRowAttribute(RowAttribute.TST)) {
                        rowAttrs[index++] = tsType.id;
                    }
                    if (hasRowAttribute(RowAttribute.EPO)) {
                        rowAttrs[index++] = leaderEpoch.orElse(null);
                    }
                    if (hasRowAttribute(RowAttribute.HDR)) {
                        rowAttrs[index++] = convertHeaders(headers);
                    }
                    rowInfo = conn.createStruct(ROWINFO, rowAttrs);
                }

                ColumnDef keyColDef = keyColDefs.get(topic);
                if (hasRowAttribute(RowAttribute.ROWKEY)) {
                    addParam(keyObj._1, keyColDef, paramMarkers, keyObj._2, params);
                }
                ColumnDef valueColDef = valueColDefs.get(topic);
                int rowValueSize = valueColDef.getColumnType() == DuckDBColumnType.STRUCT
                    ? ((StructColumnDef) valueColDef).getColumnDefs().size()
                    : 1;
                if (valueObj._2 instanceof Struct
                    && ((Struct) valueObj._2).getAttributes().length == rowValueSize) {
                    Object[] values = ((Struct) valueObj._2).getAttributes();
                    StructColumnDef structColumnDef = (StructColumnDef) valueColDef;
                    int i = 0;
                    for (ColumnDef columnDef : structColumnDef.getColumnDefs().values()) {
                        addParam(valueObj._1, columnDef, paramMarkers, values[i++], params);
                    }
                } else {
                    addParam(valueObj._1, valueColDef, paramMarkers, valueObj._2, params);
                }
                if (rowInfoSize > 0) {
                    paramMarkers.add("?");
                    params.add(rowInfo);
                }

                sql = "INSERT INTO '" + topic + "' VALUES (" + String.join(",", paramMarkers) + ")";
                PreparedStatement stmt = stmts.computeIfAbsent(sql, s -> {
                    try {
                        return conn.prepareStatement(s);
                    } catch (SQLException e) {
                        LOG.error("Could not prepare statement: {}", s, e);
                        throw new RuntimeException("Could not prepare statement: " + s, e);
                    }
                });
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
                stmt.addBatch();
            } catch (IOException | SQLException e) {
                LOG.error("Could not execute SQL: {}", sql, e);
                throw new RuntimeException("Could not execute SQL: " + sql, e);
            } catch (Exception e) {
                LOG.error("Could not insert row: {}", valueObj != null ? valueObj._2 : null, e);
                throw new RuntimeException("Could not insert row: "
                    + (valueObj != null ? valueObj._2 : null), e);
            }
        }

        private void addParam(Context ctx, ColumnDef colDef, List<String> paramMarkers,
            Object obj, List<Object> params) {
            if (ctx != null && colDef.getColumnType() == DuckDBColumnType.UNION) {
                UnionColumnDef unionColumnDef = (UnionColumnDef) colDef;
                paramMarkers.add("union_value(\""
                    + ctx.getUnionBranch(unionColumnDef) + "\" := ?)");
            } else {
                paramMarkers.add("?");
            }
            params.add(obj);
        }

        @Override
        public void handleUpdate(Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void endBatch(int count) {
            stmts.forEach((sql, stmt) -> {
                try {
                    stmt.executeBatch();
                    stmt.close();
                } catch (SQLException e) {
                    LOG.error("Could not execute SQL: {}", sql, e);
                    throw new RuntimeException("Could not execute SQL: " + sql, e);
                }
            });
            stmts.clear();
        }

        @SuppressWarnings("unchecked")
        private Map<String, String> convertHeaders(Headers headers) {
            if (headers == null) {
                return conn.createMap("MAP(VARCHAR, VARCHAR)", Collections.emptyMap());
            }
            Map<String, String> map = new LinkedHashMap<>();
            for (Header header : headers) {
                String value = header.value() != null
                    ? new String(header.value(), StandardCharsets.UTF_8)
                    : "";
                // We only keep the last header value
                map.put(header.key(), value);
            }
            return conn.createMap("MAP(VARCHAR, VARCHAR)", map);
        }

        private static final int MAGIC_BYTE = 0x0;

        private int schemaIdFor(byte[] payload) {
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            if (buffer.get() != MAGIC_BYTE) {
                throw new UncheckedIOException(new IOException("Unknown magic byte!"));
            }
            return buffer.getInt();
        }
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    public void sync() {
        caches.forEach((key, value) -> {
            try {
                value.sync();
            } catch (Exception e) {
                LOG.warn("Could not sync cache for {}", key);
            }
        });
    }

    public KafkaCache<Bytes, Bytes> getCache(String topic) {
        return caches.get(topic);
    }

    @Override
    public void close() throws IOException {
        caches.forEach((key, value) -> {
            try {
                value.close();
            } catch (IOException e) {
                LOG.warn("Could not close cache for {}", key);
            }
        });
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.warn("Could not close DuckDB connection for {}", config.getDbUrl());
        }
        resetSchemaRegistry(config.getSchemaRegistryUrls(), schemaRegistry);
    }
}
