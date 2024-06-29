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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.kcache.KafkaCacheConfig;
import io.kcache.kawai.util.Jackson;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

public class KawaiConfig extends KafkaCacheConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KawaiConfig.class);

    public static final String LISTENER_CONFIG = "listener";
    public static final String LISTENER_DEFAULT = "http://0.0.0.0:8765";
    public static final String LISTENER_DOC =
        "The URL for kawai to listen on. The listener must include the protocol, "
            + "hostname, and port. For example: http://myhost:8765, https://0.0.0.0:8765";

    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_URL_DOC =
        "Comma-separated list of URLs for schema registry instances that can be used to register "
            + "or look up schemas.";

    public static final String TOPICS_CONFIG = "topics";
    public static final String TOPICS_DOC = "Comma-separated list of topics.";

    public static final String KEY_SERDES_CONFIG = "key.serdes";
    public static final String KEY_SERDES_DOC =
        "Comma-separated list of \"<topic>=<serde>\" "
            + "settings, where \"serde\" is the serde to use for topic keys, "
            + "which must be one of [short, int, long, float, double, string, "
            + "binary, avro:<schema|@file>, json:<schema|@file>, proto:<schema|@file>, "
            + "latest (use latest version in SR), <id> (use schema id from SR)]. "
            + "Default: binary";

    public static final String VALUE_SERDES_CONFIG = "value.serdes";
    public static final String VALUE_SERDES_DOC =
        "Comma-separated list of \"<topic>=<serde>\" "
            + "settings, where \"serde\" is the serde to use for topic values, "
            + "which must be one of [short, int, long, float, double, string, "
            + "binary, avro:<schema|@file>, json:<schema|@file>, proto:<schema|@file>, "
            + "latest (use latest version in SR), <id> (use schema id from SR)]. "
            + "Default: latest";

    public static final String STAGE_SCHEMAS_CONFIG = "stage.schemas";
    public static final String STAGE_SCHEMAS_DOC =
        "Comma-separated list of schemas, one of avro:<schema|@file>, json:<schema|@file>, "
            + "proto:<schema|@file>.  If more then one schema is specified, later ones are "
            + "checked for backward compatibility against earlier ones.";

    public static final String GRAPHQL_MAX_COMPLEXITY_CONFIG = "graphql.max.complexity";
    public static final int GRAPHQL_MAX_COMPLEXITY_DEFAULT = Integer.MAX_VALUE;
    public static final String GRAPHQL_MAX_COMPLEXITY_DOC =
        "The maximum complexity of the fields for a GraphQL query.";

    public static final String GRAPHQL_MAX_DEPTH_CONFIG = "graphql.max.depth";
    public static final int GRAPHQL_MAX_DEPTH_DEFAULT = Integer.MAX_VALUE;
    public static final String GRAPHQL_MAX_DEPTH_DOC =
        "The maximum depth for a GraphQL query.";

    public static final String GRAPHQL_TIMEOUT_MS_CONFIG = "graphql.timeout.ms";
    public static final int GRAPHQL_TIMEOUT_MS_DEFAULT = Integer.MAX_VALUE;
    public static final String GRAPHQL_TIMEOUT_MS_DOC =
        "The timeout in ms for a GraphQL query.";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_LOCATION_DOC =
        "Location of the keystore file to use for SSL. This is required for HTTPS.";
    public static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEYSTORE_PASSWORD_DOC =
        "The store password for the keystore file.";
    public static final String SSL_KEYSTORE_PASSWORD_DEFAULT = "";

    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    public static final String SSL_KEY_PASSWORD_DOC =
        "The password of the private key in the keystore file.";
    public static final String SSL_KEY_PASSWORD_DEFAULT = "";

    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_TYPE_DOC =
        "The type of keystore file.";

    public static final String SSL_STORE_TYPE_JKS = "JKS";
    public static final String SSL_STORE_TYPE_PKCS12 = "PKCS12";
    public static final ConfigDef.ValidString SSL_STORE_TYPE_VALIDATOR =
        ConfigDef.ValidString.in(
            SSL_STORE_TYPE_JKS,
            SSL_STORE_TYPE_PKCS12
        );

    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC =
        "The algorithm used by the key manager factory for SSL connections. "
            + "Leave blank to use Jetty's default.";
    public static final String SSL_KEYMANAGER_ALGORITHM_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DOC =
        "Location of the trust store. Required only to authenticate HTTPS clients.";
    public static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC =
        "The store password for the trust store file.";
    public static final String SSL_TRUSTSTORE_PASSWORD_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DOC =
        "The type of trust store file.";
    public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = "JKS";

    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC =
        "The algorithm used by the trust manager factory for SSL connections. "
            + "Leave blank to use Jetty's default.";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DEFAULT = "";

    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String SSL_PROTOCOL_DOC =
        "The SSL protocol used to generate the SslContextFactory.";
    public static final String SSL_PROTOCOL_DEFAULT = "TLS";

    public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
    public static final String SSL_PROVIDER_DOC =
        "The SSL security provider name. Leave blank to use Jetty's default.";
    public static final String SSL_PROVIDER_DEFAULT = "";

    public static final String SSL_CLIENT_AUTHENTICATION_CONFIG = "ssl.client.authentication";
    public static final String SSL_CLIENT_AUTHENTICATION_NONE = "NONE";
    public static final String SSL_CLIENT_AUTHENTICATION_REQUESTED = "REQUESTED";
    public static final String SSL_CLIENT_AUTHENTICATION_REQUIRED = "REQUIRED";
    public static final String SSL_CLIENT_AUTHENTICATION_DOC =
        "SSL mutual auth. Set to NONE to disable SSL client authentication, set to REQUESTED to "
            + "request but not require SSL client authentication, and set to REQUIRED to require SSL "
            + "client authentication.";
    public static final ConfigDef.ValidString SSL_CLIENT_AUTHENTICATION_VALIDATOR =
        ConfigDef.ValidString.in(
            SSL_CLIENT_AUTHENTICATION_NONE,
            SSL_CLIENT_AUTHENTICATION_REQUESTED,
            SSL_CLIENT_AUTHENTICATION_REQUIRED
        );

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DOC =
        "The list of protocols enabled for SSL connections. Comma-separated list. "
            + "Leave blank to use Jetty's defaults.";
    public static final String SSL_ENABLED_PROTOCOLS_DEFAULT = "";

    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_CIPHER_SUITES_DOC =
        "A list of SSL cipher suites. Leave blank to use Jetty's defaults.";
    public static final String SSL_CIPHER_SUITES_DEFAULT = "";

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG =
        "ssl.endpoint.identification.algorithm";
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC =
        "The endpoint identification algorithm to validate the server hostname using the "
            + "server certificate.";
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT = null;

    public static final String TOKEN_TYPE_CONFIG = "token.type";
    public static final String TOKEN_TYPE_DOC =
        "The token type, either simple (for single-node cluster testing) or jwt.";
    public static final String TOKEN_TYPE_SIMPLE = "simple";
    public static final String TOKEN_TYPE_JWT = "jwt";
    public static final ConfigDef.ValidString TOKEN_TYPE_VALIDATOR =
        ConfigDef.ValidString.in(
            TOKEN_TYPE_SIMPLE,
            TOKEN_TYPE_JWT
        );

    public static final String TOKEN_PUBLIC_KEY_PATH_CONFIG = "token.public.key.path";
    public static final String TOKEN_PUBLIC_KEY_PATH_DOC =
        "Location of a PEM encoded public key for verifying tokens.";

    public static final String TOKEN_PRIVATE_KEY_PATH_CONFIG = "token.private.key.path";
    public static final String TOKEN_PRIVATE_KEY_PATH_DOC =
        "Location of a PEM encoded private key for signing tokens.";

    public static final String TOKEN_SIGNATURE_ALGORITHM_CONFIG = "token.signature.algorithm";
    public static final String TOKEN_SIGNATURE_ALGORITHM_DEFAULT = "RS256";
    public static final ConfigDef.ValidString TOKEN_SIGNATURE_ALGORITHM_VALIDATOR =
        ConfigDef.ValidString.in("RS256");
    public static final String TOKEN_SIGNATURE_ALGORITHM_DOC =
        "Signature scheme to be used when signing/verifying tokens"
            + " as defined in https://tools.ietf.org/html/rfc7518#section-3.1."
            + " Currently only RS256 is supported.";

    public static final String TOKEN_TTL_SECS_CONFIG = "token.ttl.secs";
    public static final int TOKEN_TTL_SECS_DEFAULT = 300;
    public static final String TOKEN_TTL_SECS_DOC = "Time-to-live for tokens.";

    private static final ListPropertyParser listPropertyParser = new ListPropertyParser();
    private static final MapPropertyParser mapPropertyParser = new MapPropertyParser();
    private static final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private static final ConfigDef config;

    static {
        config = baseConfigDef()
            .define(
                LISTENER_CONFIG,
                Type.STRING,
                LISTENER_DEFAULT,
                Importance.HIGH,
                LISTENER_DOC
            ).define(SCHEMA_REGISTRY_URL_CONFIG,
                Type.LIST,
                null,
                Importance.HIGH,
                SCHEMA_REGISTRY_URL_DOC
            ).define(TOPICS_CONFIG,
                Type.LIST,
                "",
                Importance.HIGH,
                TOPICS_DOC
            ).define(KEY_SERDES_CONFIG,
                Type.STRING, // use custom list parsing
                "",
                Importance.HIGH,
                KEY_SERDES_DOC
            ).define(VALUE_SERDES_CONFIG,
                Type.STRING, // use custom list parsing
                "",
                Importance.HIGH,
                VALUE_SERDES_DOC
            ).define(STAGE_SCHEMAS_CONFIG,
                Type.STRING, // use custom list parsing
                "",
                Importance.LOW,
                STAGE_SCHEMAS_DOC
            ).define(
                GRAPHQL_MAX_COMPLEXITY_CONFIG,
                Type.INT,
                GRAPHQL_MAX_COMPLEXITY_DEFAULT,
                Importance.LOW,
                GRAPHQL_MAX_COMPLEXITY_DOC
            ).define(
                GRAPHQL_MAX_DEPTH_CONFIG,
                Type.INT,
                GRAPHQL_MAX_DEPTH_DEFAULT,
                Importance.LOW,
                GRAPHQL_MAX_DEPTH_DOC
            ).define(
                GRAPHQL_TIMEOUT_MS_CONFIG,
                Type.INT,
                GRAPHQL_TIMEOUT_MS_DEFAULT,
                Importance.LOW,
                GRAPHQL_TIMEOUT_MS_DOC
            ).define(
                SSL_KEYSTORE_LOCATION_CONFIG,
                Type.STRING,
                SSL_KEYSTORE_LOCATION_DEFAULT,
                Importance.HIGH,
                SSL_KEYSTORE_LOCATION_DOC
            ).define(
                SSL_KEYSTORE_PASSWORD_CONFIG,
                Type.PASSWORD,
                SSL_KEYSTORE_PASSWORD_DEFAULT,
                Importance.HIGH,
                SSL_KEYSTORE_PASSWORD_DOC
            ).define(
                SSL_KEY_PASSWORD_CONFIG,
                Type.PASSWORD,
                SSL_KEY_PASSWORD_DEFAULT,
                Importance.HIGH,
                SSL_KEY_PASSWORD_DOC
            ).define(
                SSL_KEYSTORE_TYPE_CONFIG,
                Type.STRING,
                SSL_STORE_TYPE_JKS,
                SSL_STORE_TYPE_VALIDATOR,
                Importance.MEDIUM,
                SSL_KEYSTORE_TYPE_DOC
            ).define(
                SSL_KEYMANAGER_ALGORITHM_CONFIG,
                Type.STRING,
                SSL_KEYMANAGER_ALGORITHM_DEFAULT,
                Importance.LOW,
                SSL_KEYMANAGER_ALGORITHM_DOC
            ).define(
                SSL_TRUSTSTORE_LOCATION_CONFIG,
                Type.STRING,
                SSL_TRUSTSTORE_LOCATION_DEFAULT,
                Importance.HIGH,
                SSL_TRUSTSTORE_LOCATION_DOC
            ).define(
                SSL_TRUSTSTORE_PASSWORD_CONFIG,
                Type.PASSWORD,
                SSL_TRUSTSTORE_PASSWORD_DEFAULT,
                Importance.HIGH,
                SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(
                SSL_TRUSTSTORE_TYPE_CONFIG,
                Type.STRING,
                SSL_TRUSTSTORE_TYPE_DEFAULT,
                Importance.MEDIUM,
                SSL_TRUSTSTORE_TYPE_DOC)
            .define(
                SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                Type.STRING,
                SSL_TRUSTMANAGER_ALGORITHM_DEFAULT,
                Importance.LOW,
                SSL_TRUSTMANAGER_ALGORITHM_DOC
            ).define(
                SSL_PROTOCOL_CONFIG,
                Type.STRING,
                SSL_PROTOCOL_DEFAULT,
                Importance.MEDIUM,
                SSL_PROTOCOL_DOC)
            .define(
                SSL_PROVIDER_CONFIG,
                Type.STRING,
                SSL_PROVIDER_DEFAULT,
                Importance.MEDIUM,
                SSL_PROVIDER_DOC
            ).define(
                SSL_CLIENT_AUTHENTICATION_CONFIG,
                Type.STRING,
                SSL_CLIENT_AUTHENTICATION_NONE,
                SSL_CLIENT_AUTHENTICATION_VALIDATOR,
                Importance.MEDIUM,
                SSL_CLIENT_AUTHENTICATION_DOC
            ).define(
                SSL_ENABLED_PROTOCOLS_CONFIG,
                Type.LIST,
                SSL_ENABLED_PROTOCOLS_DEFAULT,
                Importance.MEDIUM,
                SSL_ENABLED_PROTOCOLS_DOC
            ).define(
                SSL_CIPHER_SUITES_CONFIG,
                Type.LIST,
                SSL_CIPHER_SUITES_DEFAULT,
                Importance.LOW,
                SSL_CIPHER_SUITES_DOC
            ).define(
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                Type.STRING,
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT,
                Importance.LOW,
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
            ).define(
                TOKEN_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                TOKEN_TYPE_SIMPLE,
                TOKEN_TYPE_VALIDATOR,
                ConfigDef.Importance.HIGH,
                TOKEN_TYPE_DOC
            ).define(
                TOKEN_PUBLIC_KEY_PATH_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                TOKEN_PUBLIC_KEY_PATH_DOC
            ).define(
                TOKEN_PRIVATE_KEY_PATH_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                TOKEN_PRIVATE_KEY_PATH_DOC
            ).define(
                TOKEN_SIGNATURE_ALGORITHM_CONFIG,
                ConfigDef.Type.STRING,
                TOKEN_SIGNATURE_ALGORITHM_DEFAULT,
                TOKEN_SIGNATURE_ALGORITHM_VALIDATOR,
                ConfigDef.Importance.LOW,
                TOKEN_SIGNATURE_ALGORITHM_DOC
            ).define(
                TOKEN_TTL_SECS_CONFIG,
                ConfigDef.Type.INT,
                TOKEN_TTL_SECS_DEFAULT,
                ConfigDef.Importance.LOW,
                TOKEN_TTL_SECS_DOC
            );
    }

    public KawaiConfig(File propsFile) {
        super(config, getPropsFromFile(propsFile));
    }

    public KawaiConfig(Map<?, ?> props) {
        super(config, props);
    }

    public List<String> getSchemaRegistryUrls() {
        return getList(SCHEMA_REGISTRY_URL_CONFIG);
    }

    public Set<String> getTopics() {
        return new HashSet<>(getList(TOPICS_CONFIG));
    }

    public Map<String, Serde> getKeySerdes() {
        String serdes = getString(KEY_SERDES_CONFIG);
        return mapPropertyParser.parse(serdes).entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new Serde(e.getValue())
            ));
    }

    public Map<String, Serde> getValueSerdes() {
        String serdes = getString(VALUE_SERDES_CONFIG);
        return mapPropertyParser.parse(serdes).entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new Serde(e.getValue())
            ));
    }

    public List<Serde> getStagedSchemas() {
        String schemas = getString(STAGE_SCHEMAS_CONFIG);
        return listPropertyParser.parse(schemas).stream()
            .map(Serde::new)
            .collect(Collectors.toList());
    }

    public int getGraphQLMaxComplexity() {
        return getInt(GRAPHQL_MAX_COMPLEXITY_CONFIG);
    }

    public int getGraphQLMaxDepth() {
        return getInt(GRAPHQL_MAX_DEPTH_CONFIG);
    }

    public int getGraphQLTimeoutMs() {
        return getInt(GRAPHQL_TIMEOUT_MS_CONFIG);
    }

    private static String getDefaultHost() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new ConfigException("Unknown local hostname", e);
        }
    }

    public static Properties getPropsFromFile(File propsFile) throws ConfigException {
        Properties props = new Properties();
        if (propsFile == null) {
            return props;
        }
        try (FileInputStream propStream = new FileInputStream(propsFile)) {
            props.load(propStream);
        } catch (IOException e) {
            throw new ConfigException("Could not load properties from " + propsFile, e);
        }
        return props;
    }

    public enum SerdeType {
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BINARY,
        AVRO,
        JSON,
        PROTO,
        LATEST,
        ID;

        private static final Map<String, SerdeType> lookup = new HashMap<>();

        static {
            for (SerdeType v : EnumSet.allOf(SerdeType.class)) {
                lookup.put(v.toString(), v);
            }
        }

        public static SerdeType get(String name) {
            return lookup.get(name.toLowerCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static class Serde {
        private final SerdeType serdeType;
        private final int id;
        private final String schema;
        private final String refs;

        public static final Serde KEY_DEFAULT = new Serde(SerdeType.BINARY, 0, null, null);
        public static final Serde VALUE_DEFAULT = new Serde(SerdeType.LATEST, 0, null, null);

        public Serde(String value) {
            int id = 0;
            String schema = null;
            String format = value;
            String refs = null;
            int index = value.indexOf(':');
            if (index > 0) {
                format = value.substring(0, index);
                int lastIndex = value.lastIndexOf(";refs:");
                if (lastIndex > 0) {
                    schema = value.substring(index + 1, lastIndex);
                    refs = value.substring(lastIndex + ";refs:".length());
                } else {
                    schema = value.substring(index + 1);
                }
                if (schema.isEmpty()) {
                    throw new ConfigException("Missing schema or file: " + value);
                }
            }
            SerdeType serdeType = SerdeType.get(format);
            if (serdeType == null) {
                try {
                    id = Integer.parseInt(value);
                    serdeType = SerdeType.ID;
                } catch (NumberFormatException e) {
                    throw new ConfigException("Could not parse serde: " + value, e);
                }
            }
            this.serdeType = serdeType;
            this.id = id;
            this.schema = schema;
            this.refs = refs;
        }

        public Serde(SerdeType serdeType, int id, String schema, String refs) {
            this.serdeType = serdeType;
            this.id = id;
            this.schema = schema;
            this.refs = refs;
        }

        public SerdeType getSerdeType() {
            return serdeType;
        }

        public int getId() {
            return id;
        }

        public String getSchemaType() {
            return serdeType == KawaiConfig.SerdeType.PROTO ? "PROTOBUF" : serdeType.name();
        }

        public String getSchema() {
            if (schema.startsWith("@")) {
                String file = schema.substring(1);
                try {
                    return Files.readString(Paths.get(file));
                } catch (IOException e) {
                    throw new IllegalArgumentException("Could not read file: " + file);
                }
            } else {
                return schema;
            }
        }

        public List<SchemaReference> getSchemaReferences() {
            String str;
            if (refs == null || refs.isEmpty()) {
                return Collections.emptyList();
            } else if (refs.startsWith("@")) {
                String file = schema.substring(1);
                try {
                    str = Files.readString(Paths.get(file));
                } catch (IOException e) {
                    throw new IllegalArgumentException("Could not read file: " + file);
                }
            } else {
                str = refs;
            }
            return parseRefs(str);
        }

        private static List<SchemaReference> parseRefs(String str) {
            List<SchemaReference> list;
            try {
                list = objectMapper.readValue(str, new TypeReference<>() {
                });
            } catch (Exception e) {
                throw new ConfigException("Could not parse refs " + str, e);
            }
            return list;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Serde serde = (Serde) o;
            return id == serde.id
                && serdeType == serde.serdeType
                && Objects.equals(schema, serde.schema)
                && Objects.equals(refs, serde.refs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serdeType, id, schema, refs);
        }

        @Override
        public String toString() {
            switch (serdeType) {
                case ID:
                    return String.valueOf(id);
                case AVRO:
                case JSON:
                case PROTO:
                    StringBuilder sb = new StringBuilder();
                    sb.append(serdeType);
                    sb.append(":");
                    sb.append(schema);
                    if (refs != null && !refs.isEmpty()) {
                        sb.append(";refs:");
                        sb.append(refs);
                    }
                    return sb.toString();
                default:
                    return serdeType.toString();
            }
        }
    }

    public static class ListPropertyParser {
        private static final char DELIM_CHAR = ',';
        private static final char QUOTE_CHAR = '\'';

        private final CsvMapper mapper;
        private final CsvSchema schema;

        public ListPropertyParser() {
            mapper = new CsvMapper()
                .enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING)
                .enable(CsvParser.Feature.WRAP_AS_ARRAY);
            schema = CsvSchema.builder()
                .setColumnSeparator(DELIM_CHAR)
                .setQuoteChar(QUOTE_CHAR)
                .setLineSeparator("")
                .build();
        }

        public List<String> parse(String str) {
            try {
                ObjectReader reader = mapper.readerFor(String[].class).with(schema);
                Iterator<String[]> iter = reader.readValues(str);
                String[] strings = iter.hasNext() ? iter.next() : new String[0];
                return Arrays.asList(strings);
            } catch (IOException e) {
                throw new IllegalArgumentException("Could not parse string " + str, e);
            }
        }

        public String asString(List<String> list) {
            try {
                String[] array = list.toArray(new String[0]);
                ObjectWriter writer = mapper.writerFor(Object[].class).with(schema);
                return writer.writeValueAsString(array);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Could not parse list " + list, e);
            }
        }
    }

    public static class MapPropertyParser {
        private final ListPropertyParser parser;

        public MapPropertyParser() {
            parser = new ListPropertyParser();
        }

        public Map<String, String> parse(String str) {
            List<String> strings = parser.parse(str);
            return strings.stream()
                .collect(Collectors.toMap(
                    s -> s.substring(0, s.indexOf('=')),
                    s -> s.substring(s.indexOf('=') + 1))
                );
        }

        public String asString(Map<String, String> map) {
            List<String> entries = map.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.toList());
            return parser.asString(entries);
        }
    }
}
