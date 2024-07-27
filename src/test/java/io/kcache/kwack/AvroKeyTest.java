package io.kcache.kwack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

public class AvroKeyTest extends AbstractSchemaTest {

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
        return createSimpleRecord(123);
    }

    private IndexedRecord createSimpleRecord(int f2) {
        Schema schema = createSimpleSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "hi");
        avroRecord.put("f2", f2);
        return avroRecord;
    }

    private Schema createEnumSchema() {
        String enumSchema = "{\"name\": \"Kind\",\"namespace\": \"example.avro\",\n"
            + "   \"type\": \"enum\",\n"
            + "  \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n"
            + "}";
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(enumSchema);
    }

    private Schema createFixedSchema() {
        String fixedSchema = "{\"name\": \"Fixed\",\n"
            + "   \"type\": \"fixed\",\n"
            + "  \"size\" : 4\n"
            + "}";
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(fixedSchema);
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
                + "     {\"name\": \"decimal\", \"type\": {\"type\": \"bytes\",\n"
                + "       \"logicalType\": \"decimal\", \"precision\": 5, \"scale\": 2}},\n"
                + "     {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},\n"
                + "     {\"name\": \"date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n"
                + "     {\"name\": \"time\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}},\n"
                + "     {\"name\": \"timestamp\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}\n"
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
        avroRecord.put("decimal", new BigDecimal("123.45"));
        avroRecord.put("uuid", UUID.fromString("d21998e8-8737-432e-a83c-13768dabd821"));
        avroRecord.put("date", LocalDate.of(2024, 1, 1));
        avroRecord.put("time", LocalTime.of(8, 30, 30));
        avroRecord.put("timestamp", Instant.ofEpochSecond(1234567890L));
        return avroRecord;
    }

    @Test
    public void testComplexKey() throws IOException {
        IndexedRecord key = createComplexRecord();
        IndexedRecord value = createSimpleRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[] { key }, new Object[] { value });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> row = lm.get(0);
        Map<String, Object> m = (Map<String, Object>) row.get("rowkey");
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
        assertEquals(new BigDecimal("123.45"), m.get("decimal"));
        assertEquals(UUID.fromString("d21998e8-8737-432e-a83c-13768dabd821"), m.get("uuid"));
        assertEquals(LocalDate.of(2024, 1, 1), m.get("date"));
        assertEquals(LocalTime.of(8, 30, 30), m.get("time"));
        assertEquals(Timestamp.from(Instant.ofEpochSecond(1234567890L)), m.get("timestamp"));
    }

    @Override
    protected String getTopic() {
        return "test-avro";
    }

    @Override
    protected Class<?> getKeySerializer() {
        return io.confluent.kafka.serializers.KafkaAvroSerializer.class;
    }

    @Override
    protected Class<?> getValueSerializer() {
        return io.confluent.kafka.serializers.KafkaAvroSerializer.class;
    }

    @Override
    protected void injectKwackProperties(Properties props) {
        super.injectKwackProperties(props);
        props.put(KwackConfig.KEY_SERDES_CONFIG, getTopic() + "=latest");
    }
}
