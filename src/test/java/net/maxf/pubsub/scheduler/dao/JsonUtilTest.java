package net.maxf.pubsub.scheduler.dao;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilTest {

    @Test
    void toJson_nullMap_returnsNull() {
        assertNull(JsonUtil.toJson(null));
    }

    @Test
    void toJson_emptyMap_returnsNull() {
        assertNull(JsonUtil.toJson(new HashMap<>()));
    }

    @Test
    void toJson_validMap_returnsJsonString() {
        Map<String, String> headers = Map.of("key1", "value1", "key2", "value2");
        String json = JsonUtil.toJson(headers);

        assertNotNull(json);
        assertTrue(json.contains("\"key1\":\"value1\""));
        assertTrue(json.contains("\"key2\":\"value2\""));
    }

    @Test
    void toJson_specialChars_escapesCorrectly() {
        Map<String, String> headers = Map.of(
            "quote", "value with \"quotes\"",
            "backslash", "value with \\backslash",
            "newline", "value with \nnewline"
        );
        String json = JsonUtil.toJson(headers);

        assertNotNull(json);
        assertTrue(json.contains("\\\""));
        assertTrue(json.contains("\\\\"));
        assertTrue(json.contains("\\n"));
    }

    @Test
    void fromJson_null_returnsEmptyMap() {
        Map<String, String> result = JsonUtil.fromJson(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void fromJson_blank_returnsEmptyMap() {
        Map<String, String> result = JsonUtil.fromJson("   ");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void fromJson_emptyObject_returnsEmptyMap() {
        Map<String, String> result = JsonUtil.fromJson("{}");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void fromJson_validJson_returnsMap() {
        String json = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        Map<String, String> result = JsonUtil.fromJson(json);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
    }

    @Test
    void fromJson_specialChars_unescapesCorrectly() {
        String json = "{\"quote\":\"value with \\\"quotes\\\"\",\"backslash\":\"value with \\\\backslash\"}";
        Map<String, String> result = JsonUtil.fromJson(json);

        assertEquals("value with \"quotes\"", result.get("quote"));
        assertEquals("value with \\backslash", result.get("backslash"));
    }

    @Test
    void fromJson_invalidJson_throwsDaoException() {
        assertThrows(DaoException.class, () -> JsonUtil.fromJson("not valid json"));
    }

    @Test
    void roundTrip_preservesData() {
        Map<String, String> original = new HashMap<>();
        original.put("X-Correlation-Id", "corr-123");
        original.put("X-Request-Id", "req-456");
        original.put("Special", "value with \"quotes\" and \\backslash");

        String json = JsonUtil.toJson(original);
        Map<String, String> restored = JsonUtil.fromJson(json);

        assertEquals(original, restored);
    }
}
