/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache.kwack.util;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.kcache.kwack.KwackConfig;
import io.kcache.kwack.KwackEngine;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
public abstract class LocalClusterTestHarness extends ClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(LocalClusterTestHarness.class);

    protected static final String MOCK_URL = "mock://kwack";

    protected Properties props;

    protected Integer serverPort;
    protected KwackEngine engine;

    public LocalClusterTestHarness() {
        super();
    }

    public LocalClusterTestHarness(int numBrokers) {
        super(numBrokers);
    }

    public KwackEngine getEngine() {
        return engine;
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        Thread.sleep(1000);

        setUpServer();
        List<SchemaProvider> providers = Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
        );
        SchemaRegistryClient schemaRegistry = KwackEngine.createSchemaRegistry(
            Collections.singletonList(MOCK_URL), providers, null);
        registerInitialSchemas(schemaRegistry);
    }

    private void setUpServer() {
        try {
            props = new Properties();
            injectKwackProperties(props);

            KwackConfig config = new KwackConfig(props);

            engine = KwackEngine.getInstance();
            engine.configure(config);
        } catch (Exception e) {
            LOG.error("Server died unexpectedly", e);
            System.exit(1);
        }
    }

    protected void registerInitialSchemas(SchemaRegistryClient schemaRegistry) throws Exception {
    }

    protected void injectKwackProperties(Properties props) {
        props.put(KwackConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KwackConfig.KAFKACACHE_TOPIC_REPLICATION_FACTOR_CONFIG, 1);
        props.put(KwackConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_URL);
        props.put(KwackConfig.DB_CONFIG, ":memory:?cache=private");
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true");
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            KwackEngine.closeInstance();
        } catch (Exception e) {
            LOG.warn("Exception during tearDown", e);
        }
        super.tearDown();
    }
}
