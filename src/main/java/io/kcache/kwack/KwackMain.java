package io.kcache.kwack;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kcache.KafkaCacheConfig;
import io.kcache.kwack.KwackConfig.ListPropertyParser;
import io.kcache.kwack.KwackConfig.MapPropertyParser;
import io.kcache.kwack.KwackConfig.RowAttribute;
import io.kcache.kwack.util.Jackson;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.io.PrintWriter;
import java.util.EnumSet;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

@Command(name = "kwack", mixinStandardHelpOptions = true,
    versionProvider = KwackMain.ManifestVersionProvider.class,
    description = "Command-line Analytics for Kafka using DuckDB.",
    sortOptions = false, sortSynopsis = false)
public class KwackMain implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(KwackMain.class);

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    private static final ListPropertyParser listPropertyParser = new ListPropertyParser();
    private static final MapPropertyParser mapPropertyParser = new MapPropertyParser();

    private KwackConfig config;

    @Option(names = {"-t", "--topic"},
        description = "Topic(s) to consume from and produce to", paramLabel = "<topic>")
    private List<String> topics;

    @Option(names = {"-p", "--partition"},
        description = "Partition(s)", paramLabel = "<partition>")
    private List<Integer> partitions;

    @Option(names = {"-b", "--bootstrap-server"},
        description = "Bootstrap broker(s) (host:[port])", paramLabel = "<broker>")
    private List<String> bootstrapBrokers;

    @Option(names = {"-m", "--metadata-timeout"},
        description = "Metadata (et.al.) request timeout", paramLabel = "<ms>")
    private Integer initTimeout;

    @Option(names = {"-F", "--file"},
        description = "Read configuration properties from file", paramLabel = "<config-file>")
    private File configFile;

    @Option(names = {"-o", "--offset"},
        description = "Offset to start consuming from:\n"
            + "  beginning | end |\n"
            + "  <value>  (absolute offset) |\n"
            + "  -<value> (relative offset from end)\n"
            + "  @<value> (timestamp in ms to start at)\n"
            + "  Default: beginning")
    private KafkaCacheConfig.Offset offset;

    @Option(names = {"-k", "--key-serde"},
        description = "(De)serialize keys using <serde>", paramLabel = "<topic=serde>")
    private Map<String, KwackConfig.Serde> keySerdes;

    @Option(names = {"-v", "--value-serde"},
        description = "(De)serialize values using <serde>\n"
            + "Available serdes:\n"
            + "  short | int | long | float |\n"
            + "  double | string | binary |\n"
            + "  avro:<schema|@file> |\n"
            + "  json:<schema|@file> |\n"
            + "  proto:<schema|@file> |\n"
            + "  latest (use latest version in SR) |\n"
            + "  <id>   (use schema id from SR)\n"
            + "  Default for key:   binary\n"
            + "  Default for value: latest\n"
            + "The avro/json/proto serde formats can\n"
            + "also be specified with refs, e.g.\n"
            + "  avro:<schema|@file>;refs:<refs|@file>\n"
            + "where refs are schema references\n"
            + "of the form \n"
            + "  [{name=\"<name>\",subject=\"<subject>\",\n"
            + "    version=<version>},..]",
        paramLabel = "<topic=serde>")
    private Map<String, KwackConfig.Serde> valueSerdes;

    @Option(names = {"-r", "--schema-registry-url"},
        description = "SR (Schema Registry) URL", paramLabel = "<url>")
    private String schemaRegistryUrl;

    @Option(names = {"-q", "--query"},
        description = "SQL query to execute. If none is specified, interactive sqlline mode is used",
        paramLabel = "<query>")
    private String query;

    @Option(names = {"-a", "--row-attributes"},
        description = "Row attribute(s) to show: rowkey (record key), keysch (key schema id), "
            + "valsch (value schema id), part (partition), off (offset), ts (timestamp), "
            + "tstype (timestamp type), epoch (leadership epoch), hdrs (headers)\n"
        + "  Default: rowkey,keysch,valsch,part,off,ts,hdrs", paramLabel = "<attr>")
    private EnumSet<RowAttribute> rowAttrs;

    @Option(names = {"-d", "--db"},
        description = "DuckDB db, appended to 'jdbc:duckdb:' Default: :memory:", paramLabel = "<db>")
    private String db;

    @Option(names = {"-X", "--property"},
        description = "Set configuration property.", paramLabel = "<prop=val>")
    private Map<String, String> properties;

    public KwackMain() {
    }

    public KwackMain(KwackConfig config) {
        this.config = config;
    }

    public URI getListener() throws URISyntaxException {
        return new URI(config.getString(KwackConfig.LISTENER_CONFIG));
    }

    @Override
    public Integer call() throws Exception {
        if (configFile != null) {
            config = new KwackConfig(configFile);
        }
        config = updateConfig();

        KwackEngine engine = KwackEngine.getInstance();
        engine.configure(config);
        engine.init();
        Observable<Map<String, Object>> obs = engine.start();
        PrintWriter pw = new PrintWriter(System.out);
        Disposable d = obs.map(MAPPER::writeValueAsString).subscribe(
            pw::println,
            pw::println,
            pw::flush
        );
        return 0;
    }

    private KwackConfig updateConfig() {
        Map<String, String> props = new HashMap<>();
        if (config != null) {
            props.putAll(config.originalsStrings());
        }
        if (topics != null) {
            props.put(KwackConfig.TOPICS_CONFIG, String.join(",", topics));
        }
        if (partitions != null) {
            props.put(KwackConfig.KAFKACACHE_TOPIC_PARTITIONS_CONFIG, partitions.stream()
                .map(Object::toString)
                .collect(Collectors.joining(",")));
        }
        if (bootstrapBrokers != null) {
            props.put(
                KwackConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapBrokers));
        }
        if (initTimeout != null) {
            props.put(KwackConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, String.valueOf(initTimeout));
        }
        if (offset != null) {
            props.put(KwackConfig.KAFKACACHE_TOPIC_PARTITIONS_OFFSET_CONFIG, offset.toString());
        }
        if (keySerdes != null) {
            props.put(KwackConfig.KEY_SERDES_CONFIG,
                mapPropertyParser.asString(keySerdes.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()))
                ));
        }
        if (valueSerdes != null) {
            props.put(KwackConfig.VALUE_SERDES_CONFIG,
                mapPropertyParser.asString(valueSerdes.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()))
                ));
        }
        if (query != null) {
            props.put(KwackConfig.QUERY_CONFIG, query);
        }
        if (rowAttrs != null) {
            props.put(KwackConfig.ROW_ATTRIBUTES_CONFIG, rowAttrs.stream()
                .map(Enum::name)
                .collect(Collectors.joining(",")));
        }
        if (db != null) {
            props.put(KwackConfig.DB_CONFIG, db);
        }
        if (schemaRegistryUrl != null) {
            props.put(KwackConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        }
        if (properties != null) {
            props.putAll(properties);
        }
        return new KwackConfig(props);
    }

    static class OffsetConverter implements CommandLine.ITypeConverter<KafkaCacheConfig.Offset> {
        @Override
        public KafkaCacheConfig.Offset convert(String value) {
            try {
                return new KafkaCacheConfig.Offset(value);
            } catch (ConfigException e) {
                throw new CommandLine.TypeConversionException("expected one of [beginning, end, "
                    + "<value>, -<value>, @<value>] but was '" + value + "'");
            }
        }
    }

    static class SerdeConverter implements CommandLine.ITypeConverter<KwackConfig.Serde> {
        @Override
        public KwackConfig.Serde convert(String value) {
            try {
                return new KwackConfig.Serde(value);
            } catch (ConfigException e) {
                throw new CommandLine.TypeConversionException("expected one of [short, int, "
                    + "long, float, double, string, binary, latest, <id>] but was '"
                    + value + "'");
            }
        }
    }

    static class ManifestVersionProvider implements CommandLine.IVersionProvider {
        public String[] getVersion() throws Exception {
            Enumeration<URL> resources = CommandLine.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                try {
                    Manifest manifest = new Manifest(url.openStream());
                    if (isApplicableManifest(manifest)) {
                        Attributes attr = manifest.getMainAttributes();
                        return new String[]{
                            "kwack - Command-line Analytics for Kafka using DuckDB",
                            "https://github.com/rayokota/kwack",
                            "Copyright (c) 2024, Robert Yokota",
                            "Version " + get(attr, "Implementation-Version")
                        };
                    }
                } catch (IOException ex) {
                    return new String[]{"Unable to read from " + url + ": " + ex};
                }
            }
            return new String[0];
        }

        private boolean isApplicableManifest(Manifest manifest) {
            Attributes attributes = manifest.getMainAttributes();
            return "kwack".equals(get(attributes, "Implementation-Title"));
        }

        private static Object get(Attributes attributes, String key) {
            return attributes.get(new Attributes.Name(key));
        }
    }

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new KwackMain());
        commandLine.registerConverter(KafkaCacheConfig.Offset.class, new OffsetConverter());
        commandLine.registerConverter(KwackConfig.Serde.class, new SerdeConverter());
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        commandLine.setUsageHelpLongOptionsMaxWidth(30);
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }
}
