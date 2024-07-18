package io.kcache.kwack;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.everit.json.schema.Schema;
import org.junit.jupiter.api.Test;

public class JsonSchemaTest extends AbstractSchemaTest {

    private Schema createSimpleSchema() {
        String schemaStr =
            "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Obj\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
                + "\"id\":{\"type\":\"integer\"},"
                + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]}}}";
        JsonSchema jsonSchema = new JsonSchema(schemaStr);
        return jsonSchema.rawSchema();
    }

    private Simple createSimpleObj() {
        Simple simple = new Simple();
        simple.setId(123);
        simple.setName("hi");
        return simple;
    }

    private Schema createComplexSchema() {
        String schemaStr =
            "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Obj\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\n"
                + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
                + "\"mystring\":{\"type\":\"string\"},"
                + "\"myint\":{\"type\":\"integer\"},"
                + "\"mylong\":{\"type\":\"integer\"},"
                + "\"myfloat\":{\"type\":\"number\"},"
                + "\"mydouble\":{\"type\":\"number\"},"
                + "\"myboolean\":{\"type\":\"boolean\"},"
                + "\"myenum\":{\"enum\": [\"red\", \"amber\", \"green\"]},"
                + "\"array\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/Data\"}}]},"
                + "\"map\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},\"additionalProperties\":{\"$ref\":\"#/definitions/Data\"}}]},"
                + "\"definitions\":{\"Data\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{"
                + "\"data\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]}}}}";
        JsonSchema jsonSchema = new JsonSchema(schemaStr);
        return jsonSchema.rawSchema();
    }

    private Complex createComplexObj() {
        Complex obj = new Complex("test");
        obj.setMystring("testUser");
        obj.setMyint(1);
        obj.setMylong(2L);
        obj.setMyfloat(3.0f);
        obj.setMydouble(4.0d);
        obj.setMyboolean(true);
        obj.setMyenum(Color.GREEN);
        obj.setMykind(new Kind2("kind2"));
        obj.setArray(ImmutableList.of(new Data("hi"), new Data("there")));
        obj.setMap(ImmutableMap.of("bye", new Data("there")));
        return obj;
    }

    @Test
    public void testSimple() throws IOException {
        Simple obj = createSimpleObj();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[]{obj});
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals("hi", m.get("name"));
        assertEquals(123L, m.get("id"));
    }

    @Test
    public void testComplex() throws IOException {
        Complex obj = createComplexObj();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[]{obj});
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        assertEquals("test", m.get("name"));
        assertEquals("testUser", m.get("mystring"));
        assertEquals(1L, m.get("myint"));
        assertEquals(2L, m.get("mylong"));
        assertEquals(3.0d, m.get("myfloat"));
        assertEquals(4.0d, m.get("mydouble"));
        assertEquals(true, m.get("myboolean"));
        assertEquals("GREEN", m.get("myenum"));
        assertEquals(ImmutableMap.of("kind2String", "kind2", "type", "kind2"), m.get("mykind"));
        Map<String, String> m1 = new HashMap<>();
        m1.put("data", "hi");
        Map<String, String> m2 = new HashMap<>();
        m2.put("data", "there");
        List<Map<String, String>> a1 = new ArrayList<>();
        a1.add(m1);
        a1.add(m2);
        Map<String, Map<String, String>> m4 = new HashMap<>();
        m4.put("bye", m2);
        assertEquals(a1, m.get("array"));
        var x = m.get("map");
        assertEquals(m4, m.get("map"));
    }

    @Test
    public void testBadName() throws IOException {
        BadName badName = new BadName("hi", 1, 2L);
        BadNameContainer obj = new BadNameContainer(1, badName);
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[]{obj});
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        Map<String, Object> bad = (Map<String, Object>) m.get("badName");
        assertEquals("hi", bad.get("name"));
        assertEquals(1L, bad.get("group"));
        assertEquals(2L, bad.get("order"));
    }

    @Override
    protected String getTopic() {
        return "test-json";
    }

    @Override
    protected Class<?> getValueSerializer() {
        return io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer.class;
    }

    public static class Simple {

        private int id;
        private String name;

        public Simple() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Simple simple = (Simple) o;
            return id == simple.id && Objects.equals(name, simple.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    public enum Color {
        RED, AMBER, GREEN
    }

    public static class Complex {

        private String name;
        private String mystring;
        private int myint;
        private long mylong;
        private float myfloat;
        private double mydouble;
        private boolean myboolean;
        private Color myenum;
        private Kind mykind;
        private List<Data> array = new ArrayList<>();
        private Map<String, Data> map = new HashMap<>();

        public Complex() {
        }

        public Complex(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMystring() {
            return mystring;
        }

        public void setMystring(String mystring) {
            this.mystring = mystring;
        }

        public int getMyint() {
            return myint;
        }

        public void setMyint(int myint) {
            this.myint = myint;
        }

        public long getMylong() {
            return mylong;
        }

        public void setMylong(long mylong) {
            this.mylong = mylong;
        }

        public float getMyfloat() {
            return myfloat;
        }

        public void setMyfloat(float myfloat) {
            this.myfloat = myfloat;
        }

        public double getMydouble() {
            return mydouble;
        }

        public void setMydouble(double mydouble) {
            this.mydouble = mydouble;
        }

        public boolean isMyboolean() {
            return myboolean;
        }

        public void setMyboolean(boolean myboolean) {
            this.myboolean = myboolean;
        }

        public Color getMyenum() {
            return myenum;
        }

        public void setMyenum(Color myenum) {
            this.myenum = myenum;
        }

        public Kind getMykind() {
            return mykind;
        }

        public void setMykind(Kind mykind) {
            this.mykind = mykind;
        }

        public List<Data> getArray() {
            return array;
        }

        public void setArray(List<Data> array) {
            this.array = array;
        }

        public Map<String, Data> getMap() {
            return map;
        }

        public void setMap(Map<String, Data> map) {
            this.map = map;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Complex obj = (Complex) o;
            return myint == obj.myint
                && mylong == obj.mylong
                && Float.compare(myfloat, obj.myfloat) == 0
                && Double.compare(mydouble, obj.mydouble) == 0
                && myboolean == obj.myboolean
                && myenum == obj.myenum
                && Objects.equals(name, obj.name)
                && Objects.equals(mystring, obj.mystring)
                && Objects.equals(array, obj.array)
                && Objects.equals(map, obj.map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                name, mystring, myint, mylong, myfloat, mydouble, myboolean, myenum, array, map);
        }
    }

    public static class Data {

        private String data;

        public Data() {
        }

        public Data(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Data data1 = (Data) o;
            return Objects.equals(data, data1.data);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(data);
        }
    }

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Kind1.class, name = "kind1"),
        @JsonSubTypes.Type(value = Kind2.class, name = "kind2")})
    public abstract class Kind {
    }

    public class Kind1 extends Kind {
        public Kind1(String kind1String) {
            this.kind1String = kind1String;
        }

        public final String kind1String;
    }

    public class Kind2 extends Kind {
        public Kind2(String kind2String) {
            this.kind2String = kind2String;
        }

        public final String kind2String;
    }

    public static class BadNameContainer {

        private int id;
        private BadName badName;

        public BadNameContainer(int id, BadName badName) {
            this.id = id;
            this.badName = badName;
        }

        public int getId() {
            return id;
        }

        public BadName getBadName() {
            return badName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BadNameContainer that = (BadNameContainer) o;
            return id == that.id && Objects.equals(badName, that.badName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, badName);
        }
    }

    public static class BadName {
        private String name;
        private int group;
        private long order;

        public BadName(String name, int group, long order) {
            this.name = name;
            this.group = group;
            this.order = order;
        }

        public String getName() {
            return name;
        }

        public int getGroup() {
            return group;
        }

        public long getOrder() {
            return order;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BadName badName = (BadName) o;
            return group == badName.group
                && order == badName.order
                && Objects.equals(name, badName.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, group, order);
        }
    }
}