package com.etl.connector.doris.sink.format;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DorisFormatLoaderTest {

    @Test
    void getFormatPlugin_json_returnsPlugin() {
        DorisFormatPlugin plugin = DorisFormatLoader.getFormatPlugin("json");
        assertNotNull(plugin);
        assertEquals("json", plugin.identifier());
    }

    @Test
    void getFormatPlugin_unknown_returnsNull() {
        assertNull(DorisFormatLoader.getFormatPlugin("no-such-format"));
    }

    @Test
    void supportedFormats_containsJson() {
        assertTrue(DorisFormatLoader.supportedFormats().contains("json"));
    }

    @Test
    void jsonPlugin_streamLoadProperties_setsJsonProps() {
        DorisFormatPlugin plugin = DorisFormatLoader.getFormatPlugin("json");
        assertEquals("json", plugin.streamLoadProperties().getProperty("format"));
        assertEquals("true", plugin.streamLoadProperties().getProperty("read_json_by_line"));
    }
}
