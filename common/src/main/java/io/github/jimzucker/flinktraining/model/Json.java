package io.github.jimzucker.flinktraining.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * JSON encoding for everything on the wire.
 *
 * <p>Configured for byte-stable output: field order follows the record
 * component order, and BigDecimal is written plainly rather than in scientific
 * notation. Two runs of a seeded generator have to produce identical bytes, and
 * that only holds if the encoder is deterministic too.
 */
public final class Json {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private Json() {
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialise " + value.getClass().getName(), e);
        }
    }

    public static byte[] toBytes(Object value) {
        return toJson(value).getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot deserialise into " + type.getName() + ": " + json, e);
        }
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> type) {
        return fromJson(new String(bytes, StandardCharsets.UTF_8), type);
    }
}
