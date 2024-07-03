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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.caffeine.CaffeineCache;
import io.kcache.kwack.KwackConfig.RowAttribute;
import io.kcache.kwack.KwackConfig.SerdeType;
import io.kcache.kwack.schema.ColumnDef;
import io.kcache.kwack.schema.MapColumnDef;
import io.kcache.kwack.schema.StructColumnDef;
import io.kcache.kwack.loader.Context;
import io.kcache.kwack.loader.Loader;
import io.kcache.kwack.loader.avro.AvroLoader;
import io.kcache.kwack.util.Jackson;
import io.vavr.control.Either;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.stream.IntStream;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import sqlline.BuiltInProperty;
import sqlline.SqlLine;
import sqlline.SqlLine.Status;

public class KwackEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KwackEngine.class);

    public static final String ROWKEY = "rowkey";
    public static final String ROWVAL = "rowval";
    public static final String ROWINFO = "rowinfo";

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    private KwackConfig config;
    private DuckDBConnection conn;
    private SchemaRegistryClient schemaRegistry;
    private Map<String, SchemaProvider> schemaProviders;
    private Map<String, KwackConfig.Serde> keySerdes;
    private Map<String, KwackConfig.Serde> valueSerdes;
    private String query;
    private EnumSet<RowAttribute> rowAttributes;
    private int rowInfoSize;
    private final Map<String, Either<SerdeType, ParsedSchema>> keySchemas = new HashMap<>();
    private final Map<String, Either<SerdeType, ParsedSchema>> valueSchemas = new HashMap<>();
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
            throw new RuntimeException(e);
        }
    }

    public void start() throws IOException {
        if (query != null && !query.isEmpty()) {
            try {
                PrintWriter pw = new PrintWriter(System.out);
                try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
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
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 0; i < numCols; i++) {
                            String name = colNames.get(i);
                            row.put(name, toJson(rs.getObject(i + 1)));
                        }
                        String s = MAPPER.writeValueAsString(row);
                        pw.println(s);
                    }
                }
                pw.flush();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        } else {
            start(new String[]{"-u", config.getDbUrl()}, true);
        }
    }

    public Status start(String[] args, boolean saveHistory) throws IOException {
        SqlLine sqlline = new SqlLine();
        sqlline.getOpts().set(BuiltInProperty.CONNECT_INTERACTION_MODE, "notAskCredentials");

        Status status = sqlline.begin(args, null, saveHistory);
        if (!Boolean.getBoolean("sqlline.system.exit")) {
            System.exit(status.ordinal());
        }
        return status;
    }

    private static Object toJson(Object obj) throws SQLException {
        if (obj instanceof DuckDBStruct) {
            Map<String, Object> m = new LinkedHashMap<>(((DuckDBStruct)obj).getMap());
            for (Map.Entry<String, Object> entry : m.entrySet()) {
                entry.setValue(toJson(entry.getValue()));
            }
            return m;
        } else if (obj instanceof DuckDBArray) {
            Object[] a = ((Object[])((DuckDBArray)obj).getArray()).clone();
            for (int i = 0; i < a.length; i++) {
                a[i] = toJson(a[i]);
            }
            return a;
        } else {
            return obj;
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

    public SchemaProvider getSchemaProvider(String schemaType) {
        return schemaProviders.get(schemaType);
    }

    public Either<SerdeType, ParsedSchema> getKeySchema(String topic) {
        return keySchemas.computeIfAbsent(topic, t -> getSchema(topic + "-key",
            keySerdes.getOrDefault(topic, KwackConfig.Serde.KEY_DEFAULT)));
    }

    public Either<SerdeType, ParsedSchema> getValueSchema(String topic) {
        return valueSchemas.computeIfAbsent(topic, t -> getSchema(topic + "-value",
            valueSerdes.getOrDefault(topic, KwackConfig.Serde.VALUE_DEFAULT)));
    }

    private Either<SerdeType, ParsedSchema> getSchema(String subject, KwackConfig.Serde serde) {
        SerdeType serdeType = serde.getSerdeType();
        switch (serdeType) {
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BINARY:
                return Either.left(serdeType);
            case AVRO:
            case JSON:
            case PROTO:
                return parseSchema(serde)
                    .<Either<SerdeType, ParsedSchema>>map(Either::right)
                    .orElseGet(() -> Either.left(SerdeType.BINARY));
            case LATEST:
                return getLatestSchema(subject).<Either<SerdeType, ParsedSchema>>map(Either::right)
                    .orElseGet(() -> Either.left(SerdeType.BINARY));
            case ID:
                return getSchemaById(serde.getId()).<Either<SerdeType, ParsedSchema>>map(Either::right)
                    .orElseGet(() -> Either.left(SerdeType.BINARY));
            default:
                throw new IllegalArgumentException("Illegal serde type: " + serde.getSerdeType());
        }
    }

    private Optional<ParsedSchema> parseSchema(KwackConfig.Serde serde) {
        return parseSchema(serde.getSchemaType(), serde.getSchema(), serde.getSchemaReferences());
    }

    public Optional<ParsedSchema> parseSchema(String schemaType, String schema,
        List<SchemaReference> references) {
        try {
            Schema s = new Schema(null, null, null, schemaType, references, schema);
            ParsedSchema parsedSchema =
                getSchemaProvider(schemaType).parseSchemaOrElseThrow(s, false, false);
            parsedSchema.validate(false);
            return Optional.of(parsedSchema);
        } catch (Exception e) {
            LOG.error("Could not parse schema " + schema, e);
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
            LOG.error("Could not find latest schema for subject " + subject, e);
            return Optional.empty();
        }
    }

    public Optional<ParsedSchema> getSchemaById(int id) {
        try {
            ParsedSchema schema = getSchemaRegistry().getSchemaById(id);
            return Optional.of(schema);
        } catch (Exception e) {
            LOG.error("Could not find schema with id " + id, e);
            return Optional.empty();
        }
    }

    public Object deserializeKey(String topic, byte[] bytes) throws IOException {
        return deserialize(true, topic, bytes);
    }

    public Object deserializeValue(String topic, byte[] bytes) throws IOException {
        return deserialize(false, topic, bytes);
    }

    private Object deserialize(boolean isKey, String topic, byte[] bytes) throws IOException {
        Either<SerdeType, ParsedSchema> schema =
            isKey ? getKeySchema(topic) : getValueSchema(topic);

        Deserializer<?> deserializer = getDeserializer(schema);

        Object object = deserializer.deserialize(topic, bytes);
        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            Context ctx = new Context(isKey, conn);
            Loader loader = null;
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    loader = new AvroLoader();
                    break;
                case "JSON":
                    break;
                case "PROTOBUF":
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
            ColumnDef columnDef = loader.schemaToColumnDef(ctx, parsedSchema);
            object = loader.messageToColumn(ctx, parsedSchema, object, columnDef);
        }

        return object;
    }

    public Deserializer<?> getDeserializer(Either<SerdeType, ParsedSchema> schema) {
        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    return new KafkaAvroDeserializer(getSchemaRegistry(), config.originals());
                case "JSON":
                    return new KafkaJsonSchemaDeserializer<>(getSchemaRegistry(), config.originals());
                case "PROTOBUF":
                    return new KafkaProtobufDeserializer<>(getSchemaRegistry(), config.originals());
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
        } else {
            switch (schema.getLeft()) {
                case STRING:
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
                    throw new IllegalArgumentException("Illegal type " + schema.getLeft());
            }
        }
    }

    private void initTables(DuckDBConnection conn) {
        for (String topic : config.getTopics()) {
            initTable(conn, topic);
        }
    }

    private void initTable(DuckDBConnection conn, String topic) {
        Either<SerdeType, ParsedSchema> keySchema = getKeySchema(topic);
        Either<SerdeType, ParsedSchema> valueSchema = getValueSchema(topic);

        ColumnDef keyColDef = toColumnDef(true, keySchema);
        ColumnDef valueColDef = toColumnDef(false, valueSchema);

        String valueDdl = null;
        if (valueColDef instanceof StructColumnDef) {
            StructColumnDef structColDef = (StructColumnDef) valueColDef;
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, ColumnDef> entry : structColDef.getColumnDefs().entrySet()) {
                sb.append(entry.getKey());
                sb.append(" ");
                sb.append(entry.getValue().toDdlWithStrategy());
                sb.append(", ");
            }
            valueDdl = sb.toString();
        } else {
            valueDdl = ROWVAL + " " + valueColDef.toDdlWithStrategy() + ", ";
        }

        String ddl = "";
        StructColumnDef rowInfoDef = getRowInfoDef();
        if (rowInfoSize > 0) {
            ddl = "CREATE TYPE rowinfo AS " + rowInfoDef.toDdl();
            try {
                conn.createStatement().execute(ddl);
            } catch (SQLException e) {
                LOG.warn("Could not execute DDL: " + e.getMessage());
            }
        }

        ddl = "CREATE TABLE IF NOT EXISTS " + topic + " (";
        if (rowAttributes.contains(RowAttribute.ROWKEY)) {
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
            LOG.error("Could not execute DDL: " + ddl, e);
            throw new RuntimeException(e);
        }
    }

    private ColumnDef toColumnDef(boolean isKey, Either<SerdeType, ParsedSchema> schema) {
        if (schema.isRight()) {
            Loader loader = null;
            ParsedSchema parsedSchema = schema.get();
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    loader = new AvroLoader();
                    break;
                case "JSON":
                    break;
                case "PROTOBUF":
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
            return loader.schemaToColumnDef(new Context(isKey, conn), parsedSchema);
        }
        switch (schema.getLeft()) {
            case STRING:
                return new ColumnDef(DuckDBColumnType.VARCHAR, NULL_STRATEGY);
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
                throw new IllegalArgumentException("Illegal type " + schema.getLeft());
        }
    }

    private int getRowInfoSize() {
        EnumSet<RowAttribute> copy = EnumSet.copyOf(rowAttributes);
        copy.remove(RowAttribute.ROWKEY);
        return copy.size();
    }

    private StructColumnDef getRowInfoDef() {
        LinkedHashMap<String, ColumnDef> defs = new LinkedHashMap<>();
        if (rowAttributes.contains(RowAttribute.KEYSCH)) {
            defs.put(RowAttribute.KEYSCH.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY));
        }
        if (rowAttributes.contains(RowAttribute.VALSCH)) {
            defs.put(RowAttribute.VALSCH.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY));
        }
        if (rowAttributes.contains(RowAttribute.PART)) {
            defs.put(RowAttribute.PART.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER));
        }
        if (rowAttributes.contains(RowAttribute.OFF)) {
            defs.put(RowAttribute.OFF.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.BIGINT));
        }
        if (rowAttributes.contains(RowAttribute.TS)) {
            defs.put(RowAttribute.TS.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.BIGINT));
        }
        if (rowAttributes.contains(RowAttribute.TSTYPE)) {
            defs.put(RowAttribute.TSTYPE.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.SMALLINT));
        }
        if (rowAttributes.contains(RowAttribute.EPOCH)) {
            defs.put(RowAttribute.EPOCH.name().toLowerCase(Locale.ROOT),
                new ColumnDef(DuckDBColumnType.INTEGER, NULL_STRATEGY));
        }
        if (rowAttributes.contains(RowAttribute.HDRS)) {
            defs.put(RowAttribute.HDRS.name().toLowerCase(Locale.ROOT), new MapColumnDef(
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

        public UpdateHandler(DuckDBConnection conn) {
            this.conn = conn;
        }

        public void handleUpdate(Headers headers,
                                 Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts, TimestampType tsType,
                                 Optional<Integer> leaderEpoch) {
            String topic = tp.topic();
            Integer keySchemaId = null;
            Integer valueSchemaId = null;
            Object keyObj = null;
            Object valueObj = null;

            try {
                if (key != null && key.get() != Bytes.EMPTY) {
                    if (getKeySchema(topic).isRight()) {
                        keySchemaId = schemaIdFor(key.get());
                    }
                    keyObj = deserializeKey(topic, key.get());
                }

                if (getValueSchema(topic).isRight()) {
                    valueSchemaId = schemaIdFor(value.get());
                }
                valueObj = deserializeValue(topic, value.get());
                int valueSize = valueObj instanceof Struct
                    ? ((Struct) valueObj).getAttributes().length
                    : 1;

                Struct rowInfo = null;
                if (rowInfoSize > 0) {
                    Object[] rowAttrs = new Object[rowInfoSize];
                    int index = 0;
                    if (rowAttributes.contains(RowAttribute.KEYSCH)) {
                        rowAttrs[index++] = keySchemaId;
                    }
                    if (rowAttributes.contains(RowAttribute.VALSCH)) {
                        rowAttrs[index++] = valueSchemaId;
                    }
                    if (rowAttributes.contains(RowAttribute.PART)) {
                        rowAttrs[index++] = tp.partition();
                    }
                    if (rowAttributes.contains(RowAttribute.OFF)) {
                        rowAttrs[index++] = offset;
                    }
                    if (rowAttributes.contains(RowAttribute.TS)) {
                        rowAttrs[index++] = ts;
                    }
                    if (rowAttributes.contains(RowAttribute.TSTYPE)) {
                        rowAttrs[index++] = tsType.id;
                    }
                    if (rowAttributes.contains(RowAttribute.EPOCH)) {
                        rowAttrs[index++] = leaderEpoch.orElse(null);
                    }
                    if (rowAttributes.contains(RowAttribute.HDRS)) {
                        rowAttrs[index++] = convertHeaders(headers);
                    }
                    rowInfo = conn.createStruct(ROWINFO, rowAttrs);
                }

                int paramCount = valueSize + (rowInfoSize > 0 ? 1 : 0);
                if (rowAttributes.contains(RowAttribute.ROWKEY)) {
                    paramCount++;
                }
                int index = 1;
                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + topic
                    + " VALUES (" + getParameterMarkers(paramCount) + ")")) {
                    if (rowAttributes.contains(RowAttribute.ROWKEY)) {
                        stmt.setObject(index++, keyObj);
                    }
                    if (valueObj instanceof Struct) {
                        Object[] values = ((Struct) valueObj).getAttributes();
                        for (Object v : values) {
                            stmt.setObject(index++, v);
                        }
                    } else {
                        stmt.setObject(index++, valueObj);
                    }
                    if (rowInfoSize > 0) {
                        stmt.setObject(index++, rowInfo);
                    }

                    stmt.execute();
                }
            } catch (IOException | SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private String getParameterMarkers(int count) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < count; i++) {
                sb.append("?");
                if (i < count - 1) {
                    sb.append(", ");
                }
            }
            return sb.toString();
        }

        public void handleUpdate(Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts) {
            throw new UnsupportedOperationException();
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

        private String trace(Throwable t) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(output, false, StandardCharsets.UTF_8));
            return output.toString(StandardCharsets.UTF_8);
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
                LOG.warn("Could not sync cache for " + key);
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
                LOG.warn("Could not close cache for " + key);
            }
        });
        resetSchemaRegistry(config.getSchemaRegistryUrls(), schemaRegistry);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getConfiguredInstance(String className, Map<String, ?> configs) {
        try {
            Class<T> cls = (Class<T>) Class.forName(className);
            Object o = Utils.newInstance(cls);
            if (o instanceof Configurable) {
                ((Configurable) o).configure(configs);
            }
            return cls.cast(o);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
