package com.etl.core.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseConfigTest {

    private TestConfig conf;

    @BeforeEach
    void setUp() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("strKey", "hello");
        map.put("intKey", 42);
        map.put("intStrKey", "100");
        map.put("longKey", 123L);
        map.put("longFromInt", 7);
        map.put("longStr", "9999999999");
        map.put("boolKey", true);
        map.put("boolStr", "true");
        map.put("listKey", Arrays.asList("a", 1, true));
        map.put("mapKey", new LinkedHashMap<>(new HashMap<String, Object>() {{
            put("k1", "v1");
            put("k2", 2);
        }}));
        map.put("badInt", "not-a-number");
        map.put("notList", "x");
        map.put("notMap", "x");
        conf = new TestConfig(map);
    }

    // ---------- String ----------
    @Test
    void getString_returnsString() {
        assertEquals("hello", conf.get("strKey", String.class));
    }

    @Test
    void getString_coercesNonString() {
        assertEquals("42", conf.get("intKey", String.class));
    }

    @Test
    void getString_missingReturnsNull() {
        assertNull(conf.get("missing", String.class));
    }

    @Test
    void getString_default() {
        assertEquals("x", conf.get("missing", String.class, "x"));
        assertEquals("hello", conf.get("strKey", String.class, "x"));
    }

    // ---------- Integer ----------
    @Test
    void getInteger_returnsInteger() {
        assertEquals(Integer.valueOf(42), conf.get("intKey", Integer.class));
    }

    @Test
    void getInteger_parsesString() {
        assertEquals(Integer.valueOf(100), conf.get("intStrKey", Integer.class));
    }

    @Test
    void getInteger_missingReturnsNull() {
        assertNull(conf.get("missing", Integer.class));
    }

    @Test
    void getInteger_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("badInt", Integer.class));
    }

    @Test
    void getInteger_default() {
        assertEquals(Integer.valueOf(5), conf.get("missing", Integer.class, 5));
    }

    @Test
    void getInteger_defaultDoesNotSwallowConversionError() {
        assertThrows(IllegalArgumentException.class,
                () -> conf.get("badInt", Integer.class, 99));
    }

    // ---------- Long ----------
    @Test
    void getLong_returnsLong() {
        assertEquals(Long.valueOf(123L), conf.get("longKey", Long.class));
    }

    @Test
    void getLong_promotesInteger() {
        assertEquals(Long.valueOf(7L), conf.get("longFromInt", Long.class));
    }

    @Test
    void getLong_parsesString() {
        assertEquals(Long.valueOf(9999999999L), conf.get("longStr", Long.class));
    }

    @Test
    void getLong_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("badInt", Long.class));
    }

    @Test
    void getLong_default() {
        assertEquals(Long.valueOf(10L), conf.get("missing", Long.class, 10L));
    }

    // ---------- Boolean ----------
    @Test
    void getBoolean_returnsBoolean() {
        assertTrue(conf.get("boolKey", Boolean.class));
    }

    @Test
    void getBoolean_parsesString() {
        assertTrue(conf.get("boolStr", Boolean.class));
    }

    @Test
    void getBoolean_missingReturnsNull() {
        assertNull(conf.get("missing", Boolean.class));
    }

    @Test
    void getBoolean_default() {
        assertFalse(conf.get("missing", Boolean.class, false));
        assertTrue(conf.get("boolKey", Boolean.class, false));
    }

    // ---------- List ----------
    @Test
    void getList_returnsStringList() {
        List<String> list = conf.get("listKey", List.class);
        assertEquals(Arrays.asList("a", "1", "true"), list);
    }

    @Test
    void getList_missingReturnsNull() {
        assertNull(conf.get("missing", List.class));
    }

    @Test
    void getList_notListThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("notList", List.class));
    }

    // ---------- Map ----------
    @Test
    void getMap_returnsMap() {
        Map<String, Object> map = conf.get("mapKey", Map.class);
        assertEquals("v1", map.get("k1"));
        assertEquals(2, map.get("k2"));
    }

    @Test
    void getMap_missingReturnsNull() {
        assertNull(conf.get("missing", Map.class));
    }

    @Test
    void getMap_notMapThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("notMap", Map.class));
    }

    // ---------- 不支持的类型 ----------
    @Test
    void unsupportedClassThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("strKey", Date.class));
    }

    // ---------- get(key) ----------
    @Test
    void getRaw_returnsObject() {
        assertEquals(42, conf.get("intKey"));
    }

    @Test
    void getRaw_missingReturnsNull() {
        assertNull(conf.get("missing"));
    }

    // ---------- contains ----------
    @Test
    void contains_works() {
        assertTrue(conf.contains("strKey"));
        assertFalse(conf.contains("missing"));
    }

    // ---------- null config ----------
    @Test
    void nullConfig_allMethodsReturnNullOrDefault() {
        TestConfig empty = new TestConfig(null);
        assertNull(empty.get("k", String.class));
        assertEquals("x", empty.get("k", String.class, "x"));
        assertNull(empty.get("k"));
        assertFalse(empty.contains("k"));
    }

    /** 测试用具体子类 */
    static class TestConfig extends BaseConfig {
        TestConfig(Map<String, Object> map) {
            super(map);
        }
    }
}
