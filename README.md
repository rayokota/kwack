
# kwack - In-Memory Analytics for Kafka using DuckDB

[![Build Status][github-actions-shield]][github-actions-link]

[github-actions-shield]: https://github.com/rayokota/kwack/actions/workflows/build.yml/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/kwack/actions

kwack supports in-memory analytics for Kafka data using DuckDB.

## Getting Started

Note that kwack requires Java 11 or higher. 

To run kwack, download a [release](https://github.com/rayokota/kwack/releases), unpack it.
Then change to the `kwack-${version}` directory and run the following to see the command-line options:

```bash
$ bin/kwack -h

Usage: kwack [-hV] [-t=<topic>]... [-p=<partition>]... [-b=<broker>]...
             [-m=<ms>] [-F=<config-file>] [-o=<offset>] [-k=<topic=serde>]...
             [-v=<topic=serde>]... [-r=<url>] [-q=<query>] [-a=<attr>]...
             [-d=<db>] [-X=<prop=val>]...
In-Memory Analytics for Kafka using DuckDB.
  -t, --topic=<topic>               Topic(s) to consume from and produce to
  -p, --partition=<partition>       Partition(s)
  -b, --bootstrap-server=<broker>   Bootstrap broker(s) (host:[port])
  -m, --metadata-timeout=<ms>       Metadata (et.al.) request timeout
  -F, --file=<config-file>          Read configuration properties from file
  -o, --offset=<offset>             Offset to start consuming from:
                                      beginning | end |
                                      <value>  (absolute offset) |
                                      -<value> (relative offset from end)
                                      @<value> (timestamp in ms to start at)
                                      Default: beginning
  -k, --key-serde=<topic=serde>     (De)serialize keys using <serde>
  -v, --value-serde=<topic=serde>   (De)serialize values using <serde>
                                    Available serdes:
                                      short | int | long | float |
                                      double | string | binary |
                                      avro:<schema|@file> |
                                      json:<schema|@file> |
                                      proto:<schema|@file> |
                                      latest (use latest version in SR) |
                                      <id>   (use schema id from SR)
                                      Default for key:   binary
                                      Default for value: latest
                                    The proto/latest/<id> serde formats can
                                    also take a message type name, e.g.
                                      proto:<schema|@file>;msg:<name>
                                    in case multiple message types exist
  -r, --schema-registry-url=<url>   SR (Schema Registry) URL
  -q, --query=<query>               SQL query to execute. If none is specified,
                                      interactive sqlline mode is used
  -a, --row-attribute=<attr>        Row attribute(s) to show:
                                      none
                                      rowkey (record key)
                                      ksi    (key schema id)
                                      vsi    (value schema id)
                                      top    (topic)
                                      par    (partition)
                                      off    (offset)
                                      ts     (timestamp)
                                      tst    (timestamp type)
                                      epo    (leadership epoch)
                                      hdr    (headers)
                                      Default: rowkey,ksi,vsi,par,off,ts,hdr
  -d, --db=<db>                     DuckDB db, appended to 'jdbc:duckdb:'
                                      Default: :memory:
  -x, --skip-bytes=<bytes>          Extra bytes to skip when deserializing with
                                      an external schema
  -X, --property=<prop=val>         Set configuration property.
  -h, --help                        Show this help message and exit.
  -V, --version                     Print version information and exit.
```

kwack shares many command-line options with [kcat](https://github.com/edenhill/kcat) (formerly kafkacat).
In addition, a file containing configuration properties can be used.  Simply modify 
`config/kwack.properties` to point to an existing Kafka broker and Schema
Registry. Then run the following:

```bash
# Run with properties file
$ bin/kwack -F config/kwack.properties
```

Starting kwack is as easy as specifying a Kafka broker, topic, and Schema Registry URL:

```bash
$ bin/kwack -b mybroker -t mytopic -r http://schema-registry-url:8081
Welcome to kwack!
Enter "!help" for usage hints.

      ___(.)>
~~~~~~\___)~~~~~~

jdbc:duckdb::memory:>
```

When kwack starts, it will enter interactive mode, where you can enter SQL queries 
to analyze Kafka data.  For non-interactive mode, specify a query on the command line:

```bash
$ bin/kwack -b mybroker -t mytopic -r http://schema-registry-url:8081 -q "SELECT * FROM mytopic"
```

The output of the above command will be in JSON, and so can be piped to other commands like jq.

One can load multiple topics, and then perform a query that joins the resulting tables on a common 
column:

```bash
$ bin/kwack -b mybroker -t mytopic -t mytopic2 -r http://schema-registry-url:8081 -q "SELECT * FROM mytopic JOIN mytopic2 USING (col1)"
```

One can convert Kafka data into Parquet format by using the COPY commmand in DuckDB:

```bash
$ bin/kwack -b mybroker -t mytopic -r http://schema-registry-url:8081 -q "COPY mytopic to 'mytopic.parquet' (FORMAT 'parquet')"
```

If not using Confluent Schema Registry, one can pass an external schema:

```bash
$ bin/kwack -b mybroker -t mytopic -v mytopic=proto:@/path/to/myschema.proto"
```

For a given schema, kwack will create DuckDB columns based on
the appropriate Avro, Protobuf, or JSON Schema as follows:

|Avro | Protobuf | JSON Schema | DuckDB |
|-----|----------|-------------|--------|
|boolean | boolean | boolean | BOOLEAN |
|int | int32, sint32, sfixed32 || INTEGER |
|| uint32, fixed32 || UINTEGER |
|long | int64. sint64, sfixed64 | integer | BIGINT |
|| uint64, fixed64 || UBIGINT |
|float | float || FLOAT |
|double | double | number | DOUBLE |
|string | string | string | VARCHAR |
|bytes, fixed | bytes || BLOB |
|enum | enum| enum | ENUM |
|record | message | object | STRUCT |
|array | repeated | array | LIST |
|map | map || MAP |
|union | oneof | oneOf,anyOf | UNION |
|decimal | confluent.type.Decimal || DECIMAL |
|date | google.type.Date || DATE |
|time-millis, time-micros | google.type.TimeOfDay || TIME |
|timestamp-millis ||| TIMESTAMP_MS |
|timestamp-micros ||| TIMESTAMP |
|timestamp-nanos | google.protobuf.Timestamp || TIMESTAMP_NS |
|duration | google.protobuf.Duration || INTERVAL |
|uuid ||| UUID |

For more on how to use kwack, see this [blog](https://yokota.blog/2024/07/11/in-memory-analytics-for-kafka-using-duckdb/).
