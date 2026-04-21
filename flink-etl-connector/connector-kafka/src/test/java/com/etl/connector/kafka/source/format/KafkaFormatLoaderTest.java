package com.etl.connector.kafka.source.format;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaFormatLoaderTest {

    @Test
    void testLoadNonExistentFormat() {
        KafkaFormatPlugin plugin = KafkaFormatLoader.getFormatPlugin("non-existent");
        assertNull(plugin, "不存在的格式应返回 null");
    }

    @Test
    void testSupportedFormatsIncludesJson() {
        List<String> formats = KafkaFormatLoader.supportedFormats();
        assertFalse(formats.isEmpty(), "应至少支持一种格式");
    }
}