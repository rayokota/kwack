package io.kcache.kwack;

import com.google.common.collect.Lists;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.kcache.kwack.util.LocalClusterTestHarness;
import io.reactivex.rxjava3.core.Observable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark for producing and reading Kafka records with Avro schema using Kwack.
 * 
 * This benchmark measures the throughput and performance of:
 * 1. Producing a configurable number of Kafka records with a simple Avro schema
 * 2. Reading and processing those records using the Kwack engine
 * 
 * To run the benchmark:
 * 
 * Quick test with just one record count (fastest, ~1 minute):
 *   mvn test-compile exec:java -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.kcache.kwack.AvroBenchmark \
 *     -Dexec.args="-p recordCount=1000"
 * 
 * Full benchmark with all record counts (~2 minutes):
 *   mvn test-compile exec:java -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.kcache.kwack.AvroBenchmark
 * 
 * Customize parameters:
 *   -Dexec.args="-p recordCount=100,1000 -wi 1 -i 2"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(0)  // Disable forking to avoid classpath issues when running from Maven
@State(Scope.Benchmark)
public class AvroBenchmark {

    @Param({"100", "1000", "10000"})
    private int recordCount;

    private BenchmarkTestHarness testHarness;
    private String topic;
    private Properties kwackProps;
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String MOCK_URL = "mock://kwack";

    /**
     * Creates a simple Avro schema for the benchmark.
     * Schema contains: id (int), name (string), value (double), timestamp (long)
     */
    private Schema createBenchmarkSchema() {
        return new Schema.Parser().parse(
            "{\"namespace\": \"io.kcache.kwack.benchmark\",\n"
                + " \"type\": \"record\",\n"
                + " \"name\": \"BenchmarkRecord\",\n"
                + " \"fields\": [\n"
                + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                + "     {\"name\": \"value\", \"type\": \"double\"},\n"
                + "     {\"name\": \"timestamp\", \"type\": \"long\"}\n"
                + "]\n"
                + "}");
    }

    /**
     * Creates a simple Avro record with the given id.
     */
    private IndexedRecord createBenchmarkRecord(int id, Random random) {
        Schema schema = createBenchmarkSchema();
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", id);
        avroRecord.put("name", "record_" + id);
        avroRecord.put("value", random.nextDouble() * 1000.0);
        avroRecord.put("timestamp", System.currentTimeMillis());
        return avroRecord;
    }

    /**
     * Inner class to configure the test harness for benchmarking.
     */
    private class BenchmarkTestHarness extends LocalClusterTestHarness {
        @Override
        protected void injectKwackProperties(Properties props) {
            super.injectKwackProperties(props);
            props.put(KwackConfig.TOPICS_CONFIG, topic);
            props.put(KwackConfig.QUERY_CONFIG, "select * from '" + topic + "'");
        }

        public String getBrokerList() {
            return brokerList;
        }
        
        public Properties getKwackProperties() {
            Properties props = new Properties();
            injectKwackProperties(props);
            return props;
        }
    }

    /**
     * Setup the test environment before the benchmark.
     * This includes starting Kafka, Schema Registry, and producing test records.
     * Note: KwackEngine initialization is NOT done here - it's measured in the benchmark.
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        topic = "benchmark-avro-" + System.currentTimeMillis();
        
        // Initialize the test harness (Kafka + Schema Registry) without starting engine
        testHarness = new BenchmarkTestHarness();
        testHarness.setUp();
        
        // Store kwack properties for use in benchmark iterations
        kwackProps = testHarness.getKwackProperties();

        // Produce records
        Properties producerProps = createProducerProps(MOCK_URL, testHarness.getBrokerList());
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(producerProps);
        
        Random random = new Random();
        for (int i = 0; i < recordCount; i++) {
            IndexedRecord record = createBenchmarkRecord(i, random);
            Object key = Bytes.wrap(ByteBuffer.allocate(4).putInt(i).array());
            ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic, key, record);
            producer.send(producerRecord);
        }
        producer.flush();
        producer.close();
    }

    /**
     * Cleanup after the benchmark.
     * Note: KwackEngine is closed in each benchmark iteration, not here.
     */
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        // Close the instance one final time to ensure cleanup
        KwackEngine.closeInstance();
        
        if (testHarness != null) {
            testHarness.tearDown();
        }
    }

    /**
     * Creates producer properties for Kafka with Avro serialization.
     */
    private Properties createProducerProps(String schemaRegistryUrl, String brokerList) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                  org.apache.kafka.common.serialization.BytesSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                  io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        return props;
    }

    /**
     * Benchmark: Initialize Kwack engine, read and process all records, then close.
     * This measures the full cycle including initialization time.
     */
    @Benchmark
    public int readRecordsWithKwack() throws IOException {
        // Get a fresh KwackEngine instance
        KwackEngine engine = KwackEngine.getInstance();
        
        try {
            // Configure the engine
            KwackConfig config = new KwackConfig(kwackProps);
            engine.configure(config);
            
            // Initialize the engine (this is measured)
            engine.init();
            
            // Start and read records (this is measured)
            Observable<Map<String, Object>> obs = engine.start();
            List<Map<String, Object>> results = Lists.newArrayList(obs.blockingIterable().iterator());
            
            // Verify we got all records
            if (results.size() != recordCount) {
                throw new RuntimeException("Expected " + recordCount + " records but got " + results.size());
            }
            
            return results.size();
        } finally {
            // Close the engine instance for the next iteration
            KwackEngine.closeInstance(false);
        }
    }

    /**
     * Main method to run the benchmark standalone.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(AvroBenchmark.class.getSimpleName())
            .forks(0)  // Use annotation-based fork value
            .build();

        new Runner(opt).run();
    }
}

