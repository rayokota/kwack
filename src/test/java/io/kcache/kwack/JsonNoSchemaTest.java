package io.kcache.kwack;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

public class JsonNoSchemaTest extends AbstractSchemaTest {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private Simple createSimpleObj() {
        Simple simple = new Simple();
        simple.setId(123);
        simple.setName("hi");
        return simple;
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
        produce(producer, getTopic(), new Object[]{objectMapper.writeValueAsString(obj)});
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        JsonNode node = objectMapper.readTree(m.get("rowval").toString());
        assertEquals("hi", node.get("name").asText());
        assertEquals(123L, node.get("id").asLong());
    }

    @Test
    public void testComplex() throws IOException {
        Complex obj = createComplexObj();
        Properties producerProps = createProducerProps(MOCK_URL);
        KafkaProducer producer = createProducer(producerProps);
        produce(producer, getTopic(), new Object[]{objectMapper.writeValueAsString(obj)});
        producer.close();

        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        List<Map<String, Object>> lm = Lists.newArrayList(obs.blockingIterable().iterator());
        Map<String, Object> m = lm.get(0);
        JsonNode node = objectMapper.readTree(m.get("rowval").toString());
        assertEquals("test", node.get("name").asText());
        assertEquals("testUser", node.get("mystring").asText());
        assertEquals(1L, node.get("myint").asLong());
        assertEquals(2L, node.get("mylong").asLong());
        assertEquals(3.0d, node.get("myfloat").asDouble());
        assertEquals(4.0d, node.get("mydouble").asDouble());
        assertEquals(true, node.get("myboolean").asBoolean());
        assertEquals("GREEN", node.get("myenum").asText());
        JsonNode node2 = node.get("mykind");
        assertEquals("kind2", node2.get("kind2String").asText());
        assertEquals("kind2", node2.get("type").asText());
        JsonNode node3 = node.get("array");
        JsonNode node4 = node3.get(0);
        assertEquals("hi", node4.get("data").asText());
        JsonNode node5 = node3.get(1);
        assertEquals("there", node5.get("data").asText());
        JsonNode node6 = node.get("map");
        JsonNode node7 = node6.get("bye");
        assertEquals("there", node7.get("data").asText());
    }

    @Override
    protected String getTopic() {
        return "test-json";
    }

    @Override
    protected Class<?> getValueSerializer() {
        return StringSerializer.class;
    }

    @Override
    protected void injectKwackProperties(Properties props) {
        super.injectKwackProperties(props);
        props.put(KwackConfig.VALUE_SERDES_CONFIG, getTopic() + "=json");
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