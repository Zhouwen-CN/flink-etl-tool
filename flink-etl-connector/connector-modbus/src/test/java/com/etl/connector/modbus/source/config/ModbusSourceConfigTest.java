package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
        configMap.put("slaveId", 1);
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals("192.168.1.100", result.getIp());
        assertEquals(502, result.getPort());
        assertEquals(1, result.getSlaveId());
        assertEquals(0, result.getStartAddress());
        assertEquals(10, result.getQuantity());
        assertTrue(result.isBounded());
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "10.0.0.1:502");
        configMap.put("startAddress", 100);
        configMap.put("quantity", 5);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.STREAMING);

        assertEquals(1, result.getSlaveId());
        assertEquals(1000L, result.getIntervalMs());
        assertFalse(result.isBounded());
    }

    @Test
    void testCustomIntervalMs() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "10.0.0.1:502");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 5);
        configMap.put("intervalMs", 2000);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.STREAMING);

        assertEquals(2000L, result.getIntervalMs());
    }

    @Test
    void testHostMissing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_noPort() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_portNotNumber() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:abc");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_portOutOfRange() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:99999");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testSlaveIdOutOfRange() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("slaveId", 248);
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testStartAddressNegative() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", -1);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testQuantityMissing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", 0);

        assertThrows(NullPointerException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testQuantityZero() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 0);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testAddressPlusQuantityOverflow() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", 65530);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }
}
