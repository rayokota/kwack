package io.kcache.kwack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

public class AvroTest extends AbstractSchemaTest {

    @Override
    protected Properties createProducerProps(String schemaRegistryUrl) {
        Properties props = super.createProducerProps(schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        return props;
    }

    private Schema createSimpleSchema() {
        return new Schema.Parser().parse(
            "{\"namespace\": \"namespace\",\n"
                + " \"type\": \"record\",\n"
                + " \"name\": \"test\",\n"
                + " \"fields\": [\n"
                + "     {\"name\": \"f1\", \"type\": \"string\"},\n"
                + "     {\"name\": \"f2\", \"type\": \"int\"}\n"
                + "]\n"
                + "}");
    }

    private IndexedRecord createSimpleRecord() {
        Schema schema = createSimpleSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "hi");
        avroRecord.put("f2", 123);
        return avroRecord;
    }

    private Schema createSimpleExtendedSchema() {
        return new Schema.Parser().parse(
            "{\"namespace\": \"namespace\",\n"
                + " \"type\": \"record\",\n"
                + " \"name\": \"test\",\n"
                + " \"fields\": [\n"
                + "     {\"name\": \"f1\", \"type\": \"string\"},\n"
                + "     {\"name\": \"f2\", \"type\": \"int\"},\n"
                + "     {\"name\": \"f3\", \"type\": \"string\", \"default\": \"hithere\"}\n"
                + "]\n"
                + "}");
    }

    private IndexedRecord createSimpleExtendedRecord() {
        Schema schema = createSimpleExtendedSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "hi");
        avroRecord.put("f2", 123);
        avroRecord.put("f3", "bye");
        return avroRecord;
    }

    private Schema createEnumSchema() {
        String enumSchema = "{\"name\": \"Kind\",\"namespace\": \"example.avro\",\n"
            + "   \"type\": \"enum\",\n"
            + "  \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n"
            + "}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(enumSchema);
        return schema;
    }

    private Schema createFixedSchema() {
        String fixedSchema = "{\"name\": \"Fixed\",\n"
            + "   \"type\": \"fixed\",\n"
            + "  \"size\" : 4\n"
            + "}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(fixedSchema);
        return schema;
    }

    private Schema createComplexSchema() {
        return new Schema.Parser().parse(
            "{\"namespace\": \"namespace\",\n"
                + " \"type\": \"record\",\n"
                + " \"name\": \"test\",\n"
                + " \"fields\": [\n"
                + "     {\"name\": \"null\", \"type\": \"null\"},\n"
                + "     {\"name\": \"boolean\", \"type\": \"boolean\"},\n"
                + "     {\"name\": \"int\", \"type\": \"int\"},\n"
                + "     {\"name\": \"long\", \"type\": \"long\"},\n"
                + "     {\"name\": \"float\", \"type\": \"float\"},\n"
                + "     {\"name\": \"double\", \"type\": \"double\"},\n"
                + "     {\"name\": \"bytes\", \"type\": \"bytes\"},\n"
                + "     {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},\n"
                + "     {\"name\": \"enum\",\n"
                + "       \"type\": {\n"
                + "         \"name\": \"Kind\",\n"
                + "         \"type\": \"enum\",\n"
                + "         \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n"
                + "       }\n"
                + "     },\n"
                + "     {\"name\": \"array\",\n"
                + "       \"type\": {\n"
                + "         \"type\": \"array\",\n"
                + "         \"items\" : \"string\"\n"
                + "       }\n"
                + "     },\n"
                + "     {\"name\": \"map\",\n"
                + "       \"type\": {\n"
                + "         \"type\": \"map\",\n"
                + "         \"values\" : \"string\"\n"
                + "       }\n"
                + "     },\n"
                + "     {\"name\": \"nullable_string\", \"type\": [\"null\", \"string\"]},\n"
                + "     {\"name\": \"union\", \"type\": [\"null\", \"string\", \"int\"]},\n"
                + "     {\"name\": \"fixed\",\n"
                + "       \"type\": {\n"
                + "         \"name\": \"Fixed\",\n"
                + "         \"type\": \"fixed\",\n"
                + "         \"size\" : 4\n"
                + "       }\n"
                + "     },\n"
                + "     {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}\n"
                + "]\n"
                + "}");
    }

    private IndexedRecord createComplexRecord() {
        Schema enumSchema = createEnumSchema();
        Schema fixedSchema = createFixedSchema();
        Schema schema = createComplexSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("null", null);
        avroRecord.put("boolean", true);
        avroRecord.put("int", 1);
        avroRecord.put("long", 2L);
        avroRecord.put("float", 3.0f);
        avroRecord.put("double", 4.0d);
        avroRecord.put("bytes", ByteBuffer.wrap(new byte[]{0, 1, 2}));
        avroRecord.put("string", "testUser");
        avroRecord.put("enum", new GenericData.EnumSymbol(enumSchema, "ONE"));
        avroRecord.put("array", ImmutableList.of("hi", "there"));
        avroRecord.put("map", ImmutableMap.of("bye", "there"));
        avroRecord.put("nullable_string", "zap");
        avroRecord.put("union", 123);
        avroRecord.put("fixed", new GenericData.Fixed(fixedSchema, new byte[]{0, 0, 0, 0}));
        avroRecord.put("uuid", UUID.fromString("d21998e8-8737-432e-a83c-13768dabd821"));
        return avroRecord;
    }

    public static void main(String[] args) {
        UUID u = UUID.randomUUID();
        System.out.println(u);
    }

    @Test
    public void testSimple() throws IOException {
        IndexedRecord record = createSimpleRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[] { record });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals("hi", m.get("f1"));
        assertEquals(123, m.get("f2"));
    }

    @Test
    public void testSimpleEvolved() throws IOException {
        IndexedRecord record = createSimpleRecord();
        IndexedRecord record2 = createSimpleExtendedRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[] { record, record2 });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals("hi", m.get("f1"));
        assertEquals(123, m.get("f2"));
        m = lm.get(1);
        assertEquals("hi", m.get("f1"));
        assertEquals(123, m.get("f2"));
        assertEquals("bye", m.get("f3"));
    }

    @Test
    public void testComplex() throws IOException {
        IndexedRecord record = createComplexRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[] { record });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertNull(m.get("null"));
        assertEquals(true, m.get("boolean"));
        assertEquals(1, m.get("int"));
        assertEquals(2L, m.get("long"));
        assertEquals(3.0f, m.get("float"));
        assertEquals(4.0d, m.get("double"));
        assertEquals(Base64.getEncoder().encodeToString(new byte[]{0, 1, 2}), m.get("bytes"));
        assertEquals("testUser", m.get("string"));
        assertEquals("ONE", m.get("enum"));
        assertEquals(ImmutableList.of("hi", "there"), m.get("array"));
        assertEquals(ImmutableMap.of("bye", "there"), m.get("map"));
        assertEquals("zap", m.get("nullable_string"));
        assertEquals(123, m.get("union"));
        assertEquals(Base64.getEncoder().encodeToString(new byte[]{0, 0, 0, 0}), m.get("fixed"));
        assertEquals(UUID.fromString("d21998e8-8737-432e-a83c-13768dabd821"), m.get("uuid"));
    }

    @Override
    protected String getTopic() {
        return "test-avro";
    }

    @Override
    protected Class<?> getValueSerializer() {
        return io.confluent.kafka.serializers.KafkaAvroSerializer.class;
    }
}
