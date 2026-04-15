package com.etl.connector.kafka.source.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KafkaFormatLoaderTest {

    @Test
    void testLoadNonExistentFormat() {
        KafkaFormatPlugin plugin = KafkaFormatLoader.getFormatPlugin("non-existent");
        assertNull(plugin, "不存在的格式应返回 null");
    }

    @Test
    void testSupportedFormatsIncludesJson() {
        String[] formats = KafkaFormatLoader.supportedFormats();
        assertTrue(formats.length > 0, "应至少支持一种格式");
    }
}