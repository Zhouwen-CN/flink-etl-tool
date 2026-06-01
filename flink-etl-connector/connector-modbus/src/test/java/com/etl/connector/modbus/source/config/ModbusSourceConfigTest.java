package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModbusSourceConfigTest {

    private SourceConfig createSourceConfig(Map<String, Object> configMap) {
        SourceConfig config = new SourceConfig();
        config.setType("modbus");
        config.setOutputTable("test_table");
        config.setConfig(configMap);
        return config;
    }

    @Test
    void testValidConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("deviceId", 1);
        configMap.put("address", 0);
        configMap.put("count", 10);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals("192.168.1.100", result.getIp());
        assertEquals(502, result.getPort());
        assertEquals(1, result.getDeviceId());
        assertEquals(0, result.getAddress());
        assertEquals(10, result.getCount());
        assertTrue(result.isBounded());
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "10.0.0.1:502");
        configMap.put("address", 100);
        configMap.put("count", 5);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.STREAMING);

        assertEquals(1, result.getDeviceId());
        assertEquals(1000L, result.getIntervalMs());
        assertFalse(result.isBounded());
    }

    @Test
    void testCustomIntervalMs() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "10.0.0.1:502");
        configMap.put("address", 0);
        configMap.put("count", 5);
        configMap.put("intervalMs", 2000);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.STREAMING);

        assertEquals(2000L, result.getIntervalMs());
    }

    @Test
    void testHostMissing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("address", 0);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_noPort() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100");
        configMap.put("address", 0);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_portNotNumber() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:abc");
        configMap.put("address", 0);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_portOutOfRange() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:99999");
        configMap.put("address", 0);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testDeviceIdOutOfRange() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("deviceId", 248);
        configMap.put("address", 0);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testAddressNegative() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", -1);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testCountMissing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);

        assertThrows(NullPointerException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testCountZero() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 0);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testDefaultWordSize() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals(1, result.getWordSize());
    }

    @Test
    void testWordSizeTwo() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("wordSize", 2);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals(2, result.getWordSize());
    }

    @Test
    void testWordSizeTwoCountOdd() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 5);
        configMap.put("wordSize", 2);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testWordSizeInvalidThree() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 6);
        configMap.put("wordSize", 3);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testWordSizeInvalidZero() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("wordSize", 0);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testAddressPlusCountOverflow() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 65530);
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }
}
