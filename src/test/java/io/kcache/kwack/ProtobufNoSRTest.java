package io.kcache.kwack;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.type.utils.DecimalUtils;
import io.kcache.kwack.proto.ComplexProto.Complex;
import io.kcache.kwack.proto.ComplexProto.Data;
import io.kcache.kwack.proto.ComplexProto.Kind;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

public class ProtobufNoSRTest extends AbstractSchemaTest {

    private ParsedSchema createComplexSchema() {
        String schemaStr = "syntax = \"proto3\";\n"
            + "\n"
            + "package io.kcache.kwack.proto;\n"
            + "\n"
            + "import \"confluent/meta.proto\";\n"
            + "import \"confluent/type/decimal.proto\";\n"
            + "import \"google/protobuf/timestamp.proto\";\n"
            + "import \"google/type/date.proto\";\n"
            + "import \"google/type/timeofday.proto\";\n"
            + "\n"
            + "option java_package = \"io.kcache.kwack.proto\";\n"
            + "option java_outer_classname = \"ComplexProto\";\n"
            + "option java_multiple_files = false;\n"
            + "\n"
            + "message Complex {\n"
            + "    optional string name = 1;\n"
            + "    string mystring = 2;\n"
            + "    bytes mybytes = 3;\n"
            + "    int32 myint = 4;\n"
            + "    uint32 myuint = 5;\n"
            + "    int64 mylong = 6;\n"
            + "    uint64 myulong = 7;\n"
            + "    float myfloat = 8;\n"
            + "    double mydouble = 9;\n"
            + "    bool myboolean = 10;\n"
            + "    Kind kind = 11;\n"
            + "    oneof myoneof {\n"
            + "        string myoneofstring = 12;\n"
            + "        int32 myoneofint = 13;\n"
            + "    }\n"
            + "    repeated string str_array = 14;\n"
            + "    repeated Data data_array = 15;\n"
            + "    map<string, Data> data_map = 16;\n"
            + "    confluent.type.Decimal decimal = 17 [(confluent.field_meta) = { params: [\n"
            + "      { key: \"precision\", value: \"5\" },\n"
            + "      { key: \"scale\", value: \"2\" }\n"
            + "    ]}];\n"
            + "    google.type.Date date = 18;\n"
            + "    google.type.TimeOfDay time = 19;\n"
            + "    google.protobuf.Timestamp timestamp = 20;\n"
            + "}\n"
            + "\n"
            + "message Data {\n"
            + "    string data = 1;\n"
            + "}\n"
            + "\n"
            + "enum Kind {\n"
            + "    ZERO = 0;\n"
            + "    ONE = 1;\n"
            + "    TWO = 2;\n"
            + "}";
        return new ProtobufSchema(schemaStr);
    }

    private Complex createComplexObj() {
        return Complex.newBuilder()
            .setName("test")
            .setMystring("testUser")
            .setMybytes(ByteString.copyFrom(new byte[]{0, 1, 2}))
            .setMyint(1)
            .setMyuint(2)
            .setMylong(2L)
            .setMyulong(3L)
            .setMyfloat(3.0f)
            .setMydouble(4.0d)
            .setMyboolean(true)
            .setKind(Kind.ONE)
            .setMyoneofint(5)
            .addStrArray("hi")
            .addStrArray("there")
            .addDataArray(Data.newBuilder().setData("hi").build())
            .addDataArray(Data.newBuilder().setData("there").build())
            .putDataMap("bye", Data.newBuilder().setData("there").build())
            .setDecimal(DecimalUtils.fromBigDecimal(new BigDecimal("123.45")))
            .setDate(Date.newBuilder().setYear(2024).setMonth(1).setDay(1).build())
            .setTime(TimeOfDay.newBuilder().setHours(12).setMinutes(30).setSeconds(30).build())
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567890L).build())
            .build();
    }

    @Test
    public void testComplex() throws IOException {
        Complex obj = createComplexObj();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[] { obj.toByteArray() });
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals("test", m.get("name"));
        assertEquals("testUser", m.get("mystring"));
        assertEquals(Base64.getEncoder().encodeToString(new byte[]{0, 1, 2}), m.get("mybytes"));
        assertEquals(1, m.get("myint"));
        assertEquals(2L, m.get("myuint"));
        assertEquals(2L, m.get("mylong"));
        assertEquals(new BigInteger("3"), m.get("myulong"));
        assertEquals(3.0f, m.get("myfloat"));
        assertEquals(4.0d, m.get("mydouble"));
        assertEquals(true, m.get("myboolean"));
        assertEquals("ONE", m.get("kind"));
        assertEquals(5, m.get("myoneof"));
        assertEquals(ImmutableList.of("hi", "there"), m.get("str_array"));
        Map<String, String> m1 = new HashMap<>();
        m1.put("data", "hi");
        Map<String, String> m2 = new HashMap<>();
        m2.put("data", "there");
        List<Map<String, String>> a1 = new ArrayList<>();
        a1.add(m1);
        a1.add(m2);
        assertEquals(a1, m.get("data_array"));
        Map<String, Map<String, String>> m4 = new HashMap<>();
        m4.put("bye", m2);
        assertEquals(m4, m.get("data_map"));
        assertEquals(new BigDecimal("123.45"), m.get("decimal"));
        assertEquals(LocalDate.of(2024, 1, 1), m.get("date"));
        assertEquals(LocalTime.of(12, 30, 30), m.get("time"));
        assertEquals(java.sql.Timestamp.from(Instant.ofEpochSecond(1234567890L)), m.get("timestamp"));
    }

    @Override
    protected String getTopic() {
        return "test-proto";
    }

    @Override
    protected Class<?> getValueSerializer() {
        return ByteArraySerializer.class;
    }

    @Override
    protected void injectKwackProperties(Properties props) {
        super.injectKwackProperties(props);
        props.put(KwackConfig.VALUE_SERDES_CONFIG,
            "'" + getTopic() + "=proto:" + createComplexSchema().canonicalString() + "'");
    }
}
