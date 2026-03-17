package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EtlFieldTypeTest {

    @Test
    void fromString_shouldReturnCorrectType_forValidTypes() {
        assertEquals(EtlFieldType.STRING, EtlFieldType.fromString("string"));
        assertEquals(EtlFieldType.STRING, EtlFieldType.fromString("STRING"));
        assertEquals(EtlFieldType.STRING, EtlFieldType.fromString("String"));
        assertEquals(EtlFieldType.BOOLEAN, EtlFieldType.fromString("boolean"));
        assertEquals(EtlFieldType.INT, EtlFieldType.fromString("int"));
        assertEquals(EtlFieldType.LONG, EtlFieldType.fromString("long"));
        assertEquals(EtlFieldType.DOUBLE, EtlFieldType.fromString("double"));
        assertEquals(EtlFieldType.DECIMAL, EtlFieldType.fromString("decimal"));
        assertEquals(EtlFieldType.TIMESTAMP, EtlFieldType.fromString("timestamp"));
        assertEquals(EtlFieldType.BYTES, EtlFieldType.fromString("bytes"));
    }

    @Test
    void fromString_shouldReturnNull_forInvalidTypes() {
        assertNull(EtlFieldType.fromString("invalid"));
        assertNull(EtlFieldType.fromString(""));
        assertNull(EtlFieldType.fromString(null));
    }
}