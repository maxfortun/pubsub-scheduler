package net.maxf.pubsub.scheduler.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private JsonUtil() {}

    public static String toJson(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(headers);
        } catch (JsonProcessingException e) {
            throw new DaoException("Failed to serialize headers to JSON", e);
        }
    }

    public static Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank()) {
            return new HashMap<>();
        }
        try {
            return MAPPER.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new DaoException("Failed to parse headers from JSON: " + json, e);
        }
    }
}
