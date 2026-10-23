package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void testDefaultFloatValue() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertFalse(result.isFloatValue());
    }

    @Test
    void testFloatValueTrueWithWordSize2() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("wordSize", 2);
        configMap.put("floatValue", true);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertTrue(result.isFloatValue());
    }

    @Test
    void testFloatValueTrueWithWordSize1() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("floatValue", true);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testDefaultWordOrder() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("wordSize", 2);
        configMap.put("floatValue", true);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals(WordOrder.ABCD, result.getWordOrder());
    }

    @Test
    void testWordOrderCDABIgnoreCase() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("wordSize", 2);
        configMap.put("floatValue", true);
        configMap.put("wordOrder", "cdab");

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals(WordOrder.CDAB, result.getWordOrder());
    }

    @Test
    void testWordOrderInvalid() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);
        configMap.put("wordSize", 2);
        configMap.put("floatValue", true);
        configMap.put("wordOrder", "BADC");

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
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

    // ==================== points（间隔读取）模式 ====================

    @Test
    void testPointsMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("deviceId", 1);
        configMap.put("wordSize", 2);
        configMap.put("points", Arrays.asList(0, 8, 16, 58));

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertTrue(result.isPointsMode());
        assertEquals(Arrays.asList(0, 8, 16, 58), result.getPoints());
        assertEquals(2, result.getWordSize());
    }

    @Test
    void testPointsModeFromStringRejected() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", "0,8,16,58");

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeDeduplicate() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, 8, 0, 16, 8));

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals(Arrays.asList(0, 8, 16), result.getPoints());
    }

    @Test
    void testPointsModeEmptyArray() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", new ArrayList<>());

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeStringValueRejected() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", "");

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeNegativeElement() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, -1));

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeOverflow() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("wordSize", 2);
        configMap.put("points", Arrays.asList(65535));

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeNonNumericElement() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, "abc"));

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeNumericStringElement() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, "8"));

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals(Arrays.asList(0, 8), result.getPoints());
    }

    @Test
    void testPointsModeDecimalElementRejected() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, 8.5));

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeMutuallyExclusiveWithAddress() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, 8));
        configMap.put("address", 0);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testPointsModeMutuallyExclusiveWithCount() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("points", Arrays.asList(0, 8));
        configMap.put("count", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testRangeModeIsNotPointsMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("address", 0);
        configMap.put("count", 10);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertFalse(result.isPointsMode());
        assertNull(result.getPoints());
    }
}
