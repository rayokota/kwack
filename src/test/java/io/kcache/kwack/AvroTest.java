package io.kcache.kwack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

public class AvroTest extends AbstractSchemaTest {

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
                + "     {\"name\": \"union\", \"type\": [\"null\", \"string\"]},\n"
                + "     {\"name\": \"fixed\",\n"
                + "       \"type\": {\n"
                + "         \"name\": \"Fixed\",\n"
                + "         \"type\": \"fixed\",\n"
                + "         \"size\" : 4\n"
                + "       }\n"
                + "     }\n"
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
        avroRecord.put("union", "zap");
        avroRecord.put("fixed", new GenericData.Fixed(fixedSchema, new byte[]{0, 0, 0, 0}));
        return avroRecord;
    }

    @Test
    public void testSimple() throws IOException {
        String topic = "test-avro";
        IndexedRecord record = createSimpleRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, topic, new Object[] { record });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals("hi", m.get("f1"));
        assertEquals(123, m.get("f2"));
    }

    @Test
    public void testComplex() throws IOException {
        String topic = "test-avro";
        IndexedRecord record = createComplexRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, topic, new Object[] { record });
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
        assertEquals("zap", m.get("union"));
        assertEquals(Base64.getEncoder().encodeToString(new byte[]{0, 0, 0, 0}), m.get("fixed"));
    }

    @Override
    protected void injectKwackProperties(Properties props) {
        super.injectKwackProperties(props);
        props.put(KwackConfig.TOPICS_CONFIG, "test-avro");
        props.put(KwackConfig.QUERY_CONFIG, "select * from 'test-avro'");
    }

    @Override
    protected Class<?> getValueSerializer() {
        return io.confluent.kafka.serializers.KafkaAvroSerializer.class;
    }
}
