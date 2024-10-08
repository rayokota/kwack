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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.kcache.KafkaCacheConfig;
import io.kcache.kwack.util.Jackson;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KwackConfig extends KafkaCacheConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KwackConfig.class);

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
            + "which must be one of [short, int, long, float, double, string, json, "
            + "binary, avro:<schema|@file>, json:<schema|@file>, proto:<schema|@file>, "
            + "latest (use latest version in SR), <id> (use schema id from SR)]. "
            + "Default: binary";

    public static final String VALUE_SERDES_CONFIG = "value.serdes";
    public static final String VALUE_SERDES_DOC =
        "Comma-separated list of \"<topic>=<serde>\" "
            + "settings, where \"serde\" is the serde to use for topic values, "
            + "which must be one of [short, int, long, float, double, string, json, "
            + "binary, avro:<schema|@file>, json:<schema|@file>, proto:<schema|@file>, "
            + "latest (use latest version in SR), <id> (use schema id from SR)]. "
            + "Default: latest";

    public static final String QUERY_CONFIG = "query";
    public static final String QUERY_DOC = "SQL query to execute.";

    public static final String ROW_ATTRIBUTES_CONFIG = "row.attributes";
    public static final String ROW_ATTRIBUTES_DOC = "Row attribute(s) to show.";
    public static final String ROW_ATTRIBUTES_DEFAULT = "rowkey,ksi,vsi,par,off,ts,hdr";

    public static final String DB_CONFIG = "db";
    public static final String DB_DOC = "DuckDB db, appended to 'jdbc:duckdb:'";
    public static final String DB_DEFAULT = ":memory:";

    public static final String SKIP_BYTES_CONFIG = "skip.bytes";
    public static final String SKIP_BYTES_DOC =
        "Extra bytes to skip when deserializing with an external schema";

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
            .define(SCHEMA_REGISTRY_URL_CONFIG,
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
            ).define(QUERY_CONFIG,
                Type.STRING,
                null,
                Importance.HIGH,
                QUERY_DOC
            ).define(ROW_ATTRIBUTES_CONFIG,
                Type.LIST,
                ROW_ATTRIBUTES_DEFAULT,
                Importance.MEDIUM,
                ROW_ATTRIBUTES_DOC
            ).define(DB_CONFIG,
                Type.STRING,
                DB_DEFAULT,
                Importance.MEDIUM,
                DB_DOC
            ).define(SKIP_BYTES_CONFIG,
                Type.INT,
                0,
                Importance.LOW,
                SKIP_BYTES_DOC
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

    public KwackConfig(File propsFile) {
        super(config, getPropsFromFile(propsFile));
    }

    public KwackConfig(Map<?, ?> props) {
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

    public String getQuery() {
        return getString(QUERY_CONFIG);
    }

    public EnumSet<RowAttribute> getRowAttributes() {
        List<String> attrs = getList(ROW_ATTRIBUTES_CONFIG);
        return attrs.stream()
            .map(v -> RowAttribute.valueOf(v.toUpperCase(Locale.ROOT)))
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(RowAttribute.class)));
    }

    public String getDbUrl() {
        String db = getString(DB_CONFIG);
        if (DB_DEFAULT.equals(db)) {
            db = DB_DEFAULT + "?cache=shared";
        }
        return "jdbc:duckdb:" + db;
    }

    public int getSkipBytes() {
        return getInt(SKIP_BYTES_CONFIG);
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

    public enum RowAttribute {
        NONE,
        ROWKEY,
        KSI,
        VSI,
        TOP,
        PAR,
        OFF,
        TS,
        TST,
        EPO,
        HDR
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
        private int id;
        private final String schema;
        private final String msg;
        private final Tag tag;

        public static final Serde KEY_DEFAULT = new Serde(SerdeType.BINARY);
        public static final Serde VALUE_DEFAULT = new Serde(SerdeType.LATEST);

        public Serde(String value) {
            SerdeType serdeType = null;
            int id = 0;
            String schema = null;
            String format = value;
            String msg = null;
            Tag tag = null;
            int index = value.indexOf(':');
            if (index > 0) {
                format = value.substring(0, index);
                schema = value.substring(index + 1);
                serdeType = SerdeType.get(format);
                if (serdeType == SerdeType.PROTO) {
                    int lastIndex = value.lastIndexOf(";msg:");
                    if (lastIndex > 0) {
                        schema = value.substring(index + 1, lastIndex);
                        msg = value.substring(lastIndex + ";msg:".length());
                    }
                } else if (serdeType == SerdeType.JSON) {
                    int lastIndex = value.lastIndexOf(";tag:");
                    if (lastIndex > 0) {
                        schema = value.substring(index + 1, lastIndex);
                        tag = new Tag(value.substring(lastIndex + ";tag:".length()));
                    }
                }
            }
            if (serdeType == null) {
                serdeType = SerdeType.get(format);
            }
            if (serdeType == null) {
                try {
                    id = Integer.parseInt(value);
                    serdeType = SerdeType.ID;
                } catch (NumberFormatException e) {
                    throw new ConfigException("Could not parse serde: " + value, e);
                }
            }
            if ((schema == null || schema.isEmpty())
                && (serdeType == SerdeType.AVRO || serdeType == SerdeType.PROTO)) {
                throw new ConfigException("Missing schema or file: " + value);
            }
            this.serdeType = serdeType;
            this.id = id;
            this.schema = schema;
            this.msg = msg;
            this.tag = tag;
        }

        public Serde(SerdeType serdeType) {
            this(serdeType, 0, null, null, null);
        }

        public Serde(SerdeType serdeType, int id, String schema, String msg, Tag tag) {
            this.serdeType = serdeType;
            this.id = id;
            this.schema = schema;
            this.msg = msg;
            this.tag = tag;
        }

        public SerdeType getSerdeType() {
            return serdeType;
        }

        boolean usesSchemaRegistry() {
            return serdeType == SerdeType.LATEST
                || serdeType == SerdeType.ID;
        }

        boolean usesExternalSchema() {
            return schema != null &&
                (serdeType == SerdeType.AVRO
                    || serdeType == SerdeType.JSON
                    || serdeType == SerdeType.PROTO);
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getSchemaType() {
            return serdeType == KwackConfig.SerdeType.PROTO ? "PROTOBUF" : serdeType.name();
        }

        public String getSchema() {
            if (schema != null && schema.startsWith("@")) {
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

        public String getMessage() {
            return msg;
        }

        public Tag getTag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Serde serde = (Serde) o;
            return id == serde.id
                && serdeType == serde.serdeType
                && Objects.equals(schema, serde.schema)
                && Objects.equals(msg, serde.msg)
                && Objects.equals(tag, serde.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serdeType, id, schema, msg, tag);
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
                    if (schema != null) {
                        sb.append(":");
                        sb.append(schema);
                        if (msg != null && !msg.isEmpty()) {
                            sb.append(";msg:");
                            sb.append(msg);
                        }
                        if (tag != null) {
                            sb.append(";tag:");
                            sb.append(tag);
                        }
                    }
                    return sb.toString();
                default:
                    return serdeType.toString();
            }
        }
    }

    public static class Tag {
        private final String source;
        private final String target;

        public Tag(String tag) {
            int index = tag.indexOf("->");
            if (index > 0) {
                this.source = tag.substring(0, index);
                this.target = tag.substring(index + "->".length());
            } else {
                this.source = tag;
                this.target = tag;
            }
        }

        public String getSource() {
            return source;
        }

        public String getTarget() {
            return target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Tag tag1 = (Tag) o;
            return Objects.equals(source, tag1.source) && Objects.equals(target, tag1.target);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, target);
        }

        @Override
        public String toString() {
            return source + "->" + target;
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
                try (MappingIterator<String[]> iter = reader.readValues(str)) {
                    String[] strings = iter.hasNext() ? iter.next() : new String[0];
                    return Arrays.asList(strings);
                }
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
