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
package io.kcache.kawai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.caffeine.CaffeineCache;
import io.kcache.kawai.util.Jackson;
import io.vavr.Tuple2;
import io.vavr.control.Either;
import java.io.UncheckedIOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

public class KawaiEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KawaiEngine.class);

    public static final String REGISTERED_SCHEMAS_COLLECTION_NAME = "_registered_schemas";
    public static final String STAGED_SCHEMAS_COLLECTION_NAME = "_staged_schemas";

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    private KawaiConfig config;
    private SchemaRegistryClient schemaRegistry;
    private Map<String, SchemaProvider> schemaProviders;
    private Map<String, KawaiConfig.Serde> keySerdes;
    private Map<String, KawaiConfig.Serde> valueSerdes;
    private final Map<Tuple2<String, ProtobufSchema>, ProtobufSchema> protSchemaCache = new HashMap<>();
    private final Map<String, KafkaCache<Bytes, Bytes>> caches;
    private final AtomicBoolean initialized;

    private int idCounter = 0;

    private static KawaiEngine INSTANCE;

    public synchronized static KawaiEngine getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new KawaiEngine();
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

    private KawaiEngine() {
        caches = new HashMap<>();
        initialized = new AtomicBoolean();
    }

    public void configure(Map<String, ?> configs) {
        configure(new KawaiConfig(configs));
    }

    public void configure(KawaiConfig config) {
        this.config = config;
    }

    public void init() {
        List<SchemaProvider> providers = Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
        );
        schemaRegistry = createSchemaRegistry(
            config.getSchemaRegistryUrls(), providers, config.originals());
        schemaProviders = providers.stream()
            .collect(Collectors.toMap(SchemaProvider::schemaType, p -> p));

        keySerdes = config.getKeySerdes();
        valueSerdes = config.getValueSerdes();

        initCaches();

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new IllegalStateException("Illegal state while initializing engine. Engine "
                + "was already initialized");
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

    public int nextId() {
        return --idCounter;
    }

    private void initCaches() {
        for (String topic : config.getTopics()) {
            initCache(topic);
        }
    }

    private void initCache(String topic) {
        Map<String, Object> originals = config.originals();
        Map<String, Object> configs = new HashMap<>(originals);
        for (Map.Entry<String, Object> config : originals.entrySet()) {
            if (!config.getKey().startsWith("kafkacache.")) {
                configs.put("kafkacache." + config.getKey(), config.getValue());
            }
        }
        String groupId = (String)
            configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kawai-1");
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        KafkaCache<Bytes, Bytes> cache = new KafkaCache<>(
            new KafkaCacheConfig(configs),
            Serdes.Bytes(),
            Serdes.Bytes(),
            new UpdateHandler(),
            new CaffeineCache<>(100, Duration.ofMillis(10000), null)
        );
        cache.init();
        caches.put(topic, cache);
    }

    class UpdateHandler implements CacheUpdateHandler<Bytes, Bytes> {

        public void handleUpdate(Headers headers,
                                 Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts, TimestampType tsType,
                                 Optional<Integer> leaderEpoch) {
            String topic = tp.topic();
            int partition = tp.partition();
            String id = topic + "-" + partition + "-" + offset;

            Map<String, Object> headersObj = convertHeaders(headers);
        }

        public void handleUpdate(Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts) {
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> convertHeaders(Headers headers) {
            if (headers == null) {
                return null;
            }
            Map<String, Object> map = new HashMap<>();
            for (Header header : headers) {
                String value = new String(header.value(), StandardCharsets.UTF_8);
                map.merge(header.key(), value, (oldV, v) -> {
                    if (oldV instanceof List) {
                        ((List<String>) oldV).add((String) v);
                        return oldV;
                    } else {
                        List<String> newV = new ArrayList<>();
                        newV.add((String) oldV);
                        newV.add((String) v);
                        return newV;
                    }
                });
            }
            return map;
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
