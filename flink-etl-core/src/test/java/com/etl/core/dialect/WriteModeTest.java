package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * WriteMode 枚举测试
 */
class WriteModeTest {

    @Test
    void testCustomModeExists() {
        WriteMode mode = WriteMode.CUSTOM;
        assertNotNull(mode);
        assertEquals("CUSTOM", mode.name());
    }

    @Test
    void testAllModes() {
        WriteMode[] modes = WriteMode.values();
        assertEquals(4, modes.length);
        assertSame(WriteMode.INSERT, modes[0]);
        assertSame(WriteMode.UPSERT, modes[1]);
        assertSame(WriteMode.CDC, modes[2]);
        assertSame(WriteMode.CUSTOM, modes[3]);
    }
}
