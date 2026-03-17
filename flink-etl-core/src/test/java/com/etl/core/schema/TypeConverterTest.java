package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.*;

class TypeConverterTest {

    // === null 和空值处理 ===

    @Test
    void convert_shouldReturnNull_forNullValue() {
        assertNull(TypeConverter.convert(null, "field", EtlFieldType.STRING));
    }

    @Test
    void convert_shouldReturnNull_forEmptyString() {
        assertNull(TypeConverter.convert("", "field", EtlFieldType.STRING));
        assertNull(TypeConverter.convert("  ", "field", EtlFieldType.STRING));
    }

    // === STRING 类型 ===

    @Test
    void convert_shouldReturnString_forStringType() {
        assertEquals("hello", TypeConverter.convert("hello", "field", EtlFieldType.STRING));
        assertEquals("123", TypeConverter.convert("123", "field", EtlFieldType.STRING));
    }

    // === BOOLEAN 类型 ===

    @Test
    void convert_shouldReturnBoolean_forBooleanStrings() {
        assertTrue((Boolean) TypeConverter.convert("true", "field", EtlFieldType.BOOLEAN));
        assertTrue((Boolean) TypeConverter.convert("TRUE", "field", EtlFieldType.BOOLEAN));
        assertTrue((Boolean) TypeConverter.convert("1", "field", EtlFieldType.BOOLEAN));
        assertTrue((Boolean) TypeConverter.convert("yes", "field", EtlFieldType.BOOLEAN));

        assertFalse((Boolean) TypeConverter.convert("false", "field", EtlFieldType.BOOLEAN));
        assertFalse((Boolean) TypeConverter.convert("FALSE", "field", EtlFieldType.BOOLEAN));
        assertFalse((Boolean) TypeConverter.convert("0", "field", EtlFieldType.BOOLEAN));
        assertFalse((Boolean) TypeConverter.convert("no", "field", EtlFieldType.BOOLEAN));
    }

    @Test
    void convert_shouldThrowException_forInvalidBoolean() {
        assertThrows(TypeConversionException.class,
            () -> TypeConverter.convert("invalid", "field", EtlFieldType.BOOLEAN));
    }

    // === INT 类型 ===

    @Test
    void convert_shouldReturnInteger_forIntStrings() {
        assertEquals(123, TypeConverter.convert("123", "field", EtlFieldType.INT));
        assertEquals(-456, TypeConverter.convert("-456", "field", EtlFieldType.INT));
        assertEquals(0, TypeConverter.convert("0", "field", EtlFieldType.INT));
    }

    @Test
    void convert_shouldThrowException_forInvalidInt() {
        assertThrows(TypeConversionException.class,
            () -> TypeConverter.convert("abc", "field", EtlFieldType.INT));
    }

    @Test
    void convert_shouldReturnSameType_forAlreadyCorrectType() {
        assertEquals(123, TypeConverter.convert(123, "field", EtlFieldType.INT));
    }

    // === LONG 类型 ===

    @Test
    void convert_shouldReturnLong_forLongStrings() {
        assertEquals(123456789L, TypeConverter.convert("123456789", "field", EtlFieldType.LONG));
        assertEquals(-987654321L, TypeConverter.convert("-987654321", "field", EtlFieldType.LONG));
    }

    // === DOUBLE 类型 ===

    @Test
    void convert_shouldReturnDouble_forDoubleStrings() {
        assertEquals(3.14, (Double) TypeConverter.convert("3.14", "field", EtlFieldType.DOUBLE), 0.001);
        assertEquals(-2.5, (Double) TypeConverter.convert("-2.5", "field", EtlFieldType.DOUBLE), 0.001);
    }

    // === DECIMAL 类型 ===

    @Test
    void convert_shouldReturnBigDecimal_forDecimalStrings() {
        BigDecimal result = (BigDecimal) TypeConverter.convert("123.456", "field", EtlFieldType.DECIMAL);
        assertEquals(new BigDecimal("123.456"), result);
    }

    // === TIMESTAMP 类型 ===

    @Test
    void convert_shouldReturnLocalDateTime_forTimestampStrings() {
        LocalDateTime result = (LocalDateTime) TypeConverter.convert(
            "2024-01-15 10:30:00", "field", EtlFieldType.TIMESTAMP);
        assertEquals(2024, result.getYear());
        assertEquals(1, result.getMonthValue());
        assertEquals(15, result.getDayOfMonth());
        assertEquals(10, result.getHour());
        assertEquals(30, result.getMinute());
    }

    @Test
    void convert_shouldThrowException_forInvalidTimestamp() {
        assertThrows(TypeConversionException.class,
            () -> TypeConverter.convert("invalid-date", "field", EtlFieldType.TIMESTAMP));
    }

    // === BYTES 类型 ===

    @Test
    void convert_shouldReturnBytes_forStringInput() {
        byte[] result = (byte[]) TypeConverter.convert("hello", "field", EtlFieldType.BYTES);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void convert_shouldReturnSameBytes_forBytesInput() {
        byte[] input = "test".getBytes(StandardCharsets.UTF_8);
        byte[] result = (byte[]) TypeConverter.convert(input, "field", EtlFieldType.BYTES);
        assertSame(input, result);
    }

    // === 兼容类型检查 ===

    @Test
    void convert_shouldReturnSameValue_forCompatibleType() {
        // Integer -> INT
        assertEquals(42, TypeConverter.convert(42, "field", EtlFieldType.INT));

        // Long -> LONG
        assertEquals(100L, TypeConverter.convert(100L, "field", EtlFieldType.LONG));

        // Boolean -> BOOLEAN
        assertTrue((Boolean) TypeConverter.convert(true, "field", EtlFieldType.BOOLEAN));

        // String -> STRING
        assertEquals("test", TypeConverter.convert("test", "field", EtlFieldType.STRING));
    }
}