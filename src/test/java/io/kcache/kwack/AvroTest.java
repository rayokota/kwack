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
        return createSimpleRecord(123);
    }

    private IndexedRecord createSimpleRecord(int f2) {
        Schema schema = createSimpleSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "hi");
        avroRecord.put("f2", f2);
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

    private Schema createNullableSchema() {
        return new Schema.Parser().parse("{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"testRecord\",\n"
            + "  \"namespace\": \"com.example.test\",\n"
            + "  \"fields\": [\n"
            + "    {\"name\": \"id\", \"type\": \"long\"},\n"
            + "    {\"name\": \"title\", \"type\": [\"null\",\"string\"], \"default\": null},\n"
            + "    {\"name\": \"year\", \"type\":  [\"null\",\"int\"], \"default\": null},\n"
            + "    {\"name\": \"sales_number\", \"type\": [\"null\",\"long\"], \"default\": null},\n"
            + "    {\"name\": \"sales_amount\", \"type\": [\"null\",\"float\"], \"default\":  null},\n"
            + "    {\"name\": \"is_first_publish\", \"type\": [\"null\",\"boolean\"], \"default\": null},\n"
            + "    {\"name\": \"operation_type\", \"type\": [\"null\",\"string\"], \"default\": null}\n"
            + "  ]\n"
            + "}");
    }

    private IndexedRecord createNullableRecord() {
        Schema schema = createNullableSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", 123456789L);
        avroRecord.put("title", "John");
        avroRecord.put("year", 2021);
        avroRecord.put("sales_number", 123456789L);
        avroRecord.put("sales_amount", 1.23456792e8f);
        avroRecord.put("is_first_publish", true);
        avroRecord.put("operation_type", "INSERT");
        return avroRecord;
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
    public void testSimpleMany() throws IOException {
        int count = 10000;
        Random random = new Random();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        for (int i = 0; i < count; i++) {
            produce(producer, getTopic(), new Object[] { createSimpleRecord(random.nextInt()) });
        }
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        assertEquals(count, lm.size());
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
        assertEquals(new BigDecimal("123.45"), m.get("decimal"));
        assertEquals(UUID.fromString("d21998e8-8737-432e-a83c-13768dabd821"), m.get("uuid"));
        assertEquals(LocalDate.of(2024, 1, 1), m.get("date"));
        assertEquals(LocalTime.of(8, 30, 30), m.get("time"));
        assertEquals(Timestamp.from(Instant.ofEpochSecond(1234567890L)), m.get("timestamp"));
    }

    @Test
    public void testNullable() throws IOException {
        IndexedRecord record = createNullableRecord();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[] { record });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals(123456789L, m.get("id"));
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
