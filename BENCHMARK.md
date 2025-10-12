# Kwack Avro Benchmark

## Quick Start

### Run benchmark with default record count (100,000 records):
```bash
mvn test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=io.kcache.kwack.KwackAvroReadBenchmark
```

### Run benchmark with fewer records (faster for testing):
```bash
mvn test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=io.kcache.kwack.KwackAvroReadBenchmark \
  -Dexec.args="-p recordCount=1000"
```

### Run with multiple record counts:
```bash
mvn test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=io.kcache.kwack.KwackAvroReadBenchmark \
  -Dexec.args="-p recordCount=1000,10000,100000"
```

## Understanding the Output

After the benchmark completes, you'll see output like:

```
Benchmark                                    (recordCount)   Mode  Cnt   Score    Error  Units
KwackAvroReadBenchmark.readRecordsWithKwack         100000  thrpt    2   0.123           ops/s
```

Where:
- **Mode**: `thrpt` = throughput (operations per second)
- **Cnt**: Number of measurement iterations
- **Score**: Operations per second - complete init+read+close cycles (higher is better)
- **Units**: ops/s = complete Kwack lifecycle operations per second

For example, a score of `0.123 ops/s` means Kwack can complete the full cycle (initialize, read 100K records, close) about once every 8 seconds.

## Benchmark Configuration

- **Warmup**: 1 iteration × 3 seconds (JVM warmup phase)
- **Measurement**: 2 iterations × 5 seconds (actual performance measurement)
- **Setup**: Each test starts an embedded Kafka cluster, Schema Registry, and produces N records
- **What is Measured**: The full Kwack lifecycle for each operation:
  1. Get KwackEngine instance
  2. Configure engine
  3. Initialize engine (`init()`)
  4. Start and read all N records from Kafka
  5. Close engine instance
- **Result**: Operations per second - how many complete init+read+close cycles can be performed

## Customizing the Benchmark

You can customize the benchmark using JMH command-line options:

```bash
# Run with more iterations for more accurate results
mvn test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=io.kcache.kwack.KwackAvroReadBenchmark \
  -Dexec.args="-p recordCount=1000 -wi 2 -i 5"
```

Where:
- `-p recordCount=X`: Set record count
- `-wi N`: Number of warmup iterations
- `-i N`: Number of measurement iterations
- `-w Ns`: Warmup time per iteration (e.g., `-w 5s`)
- `-r Ns`: Measurement time per iteration (e.g., `-r 10s`)

## Notes

- The benchmark uses `@Fork(0)` which runs in the same JVM process
- First run may be slower due to Maven dependency resolution
- Results may vary based on system resources and JVM state

