package io.kcache.kwack;

import io.kcache.kwack.util.LocalClusterTestHarness;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;

public abstract class AbstractSchemaTest extends LocalClusterTestHarness {

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    protected Properties createProducerProps(String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializer());
        return props;
    }

    protected abstract String getTopic();

    protected Class<?> getKeySerializer() {
        return org.apache.kafka.common.serialization.BytesSerializer.class;
    }

    protected abstract Class<?> getValueSerializer();

    protected KafkaProducer createProducer(Properties props) {
        return new KafkaProducer(props);
    }

    protected void produce(KafkaProducer producer, String topic, Object[] objects) {
        produce(producer, topic, null, objects);
    }

    protected void produce(KafkaProducer producer, String topic, Object[] keys, Object[] values) {
        ProducerRecord<Object, Object> record;
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            Object key;
            if (keys != null) {
                key = keys[i];
            } else {
                key = Bytes.wrap(ByteBuffer.allocate(4).putInt(value.hashCode()).array());
            }
            record = new ProducerRecord<>(topic, key, value);
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
