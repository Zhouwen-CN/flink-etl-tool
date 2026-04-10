package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WriteModeTest {

    @Test
    void testCdcModeExists() {
        WriteMode mode = WriteMode.CDC;
        assertNotNull(mode);
        assertEquals("CDC", mode.name());
    }
}