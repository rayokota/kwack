package io.kcache.kwack;

import io.kcache.kwack.util.LocalClusterTestHarness;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class AbstractSchemaTest extends LocalClusterTestHarness {

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    protected Properties createProducerProps(String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializer());
        return props;
    }

    protected abstract String getTopic();

    protected abstract Class<?> getValueSerializer();

    protected KafkaProducer createProducer(Properties props) {
        return new KafkaProducer(props);
    }

    protected void produce(KafkaProducer producer, String topic, Object[] objects) {
        ProducerRecord<String, Object> record;
        for (Object object : objects) {
            record = new ProducerRecord<>(topic, object);
            producer.send(record);
        }
    }

    @Override
    protected void injectKwackProperties(Properties props) {
        super.injectKwackProperties(props);
        String topic = getTopic();
        props.put(KwackConfig.TOPICS_CONFIG, topic);
        props.put(KwackConfig.QUERY_CONFIG, "select * from '" + topic + "'");
    }
}