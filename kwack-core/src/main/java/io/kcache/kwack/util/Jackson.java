package io.kcache.kwack.util;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.TreeMap;

/**
 * A utility class for Jackson.
 */
public class Jackson {
    private Jackson() {
        /* singleton */
    }

    /**
     * Creates a new {@link ObjectMapper}.
     *
     * @return an object mapper
     */
    public static ObjectMapper newObjectMapper() {
        return newObjectMapper(false);
    }

    /**
     * Creates a new {@link ObjectMapper}.
     *
     * @param sorted whether to sort object properties
     * @return an object mapper
     */
    public static ObjectMapper newObjectMapper(boolean sorted) {
        final ObjectMapper mapper = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
            .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES) // for simpler ref specifications
            .build();

        return configure(mapper, sorted);
    }

    /**
     * Creates a new {@link ObjectMapper} with a custom
     * {@link JsonFactory}.
     *
     * @param jsonFactory instance of {@link JsonFactory} to use
     *     for the created {@link ObjectMapper} instance.
     * @return an object mapper
     */
    public static ObjectMapper newObjectMapper(JsonFactory jsonFactory) {
        final ObjectMapper mapper = JsonMapper.builder(jsonFactory)
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
            .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
            .build();

        return configure(mapper, false);
    }

    private static ObjectMapper configure(ObjectMapper mapper, boolean sorted) {
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS);
        mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.setNodeFactory(sorted
            ? new SortingNodeFactory(true)
            : JsonNodeFactory.withExactBigDecimals(true));

        return mapper;
    }

    static class SortingNodeFactory extends JsonNodeFactory {
        public SortingNodeFactory(boolean bigDecimalExact) {
            super(bigDecimalExact);
        }

        @Override
        public ObjectNode objectNode() {
            return new ObjectNode(this, new TreeMap<>());
        }
    }
}
