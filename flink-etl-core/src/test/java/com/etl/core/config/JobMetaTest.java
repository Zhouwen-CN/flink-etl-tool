package com.etl.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JobMeta 配置解析测试
 */
class JobMetaTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testParseJobMetaWithParallelism() throws Exception {
        String json = "{\"name\":\"test-job\",\"mode\":\"batch\",\"parallelism\":4}";

        JobMeta meta = mapper.readValue(json, JobMeta.class);

        assertEquals("test-job", meta.getName());
        assertEquals("batch", meta.getMode());
        assertEquals(4, meta.getParallelism());
    }

    @Test
    void testParseJobMetaWithoutParallelism() throws Exception {
        String json = "{\"name\":\"test-job\",\"mode\":\"batch\"}";

        JobMeta meta = mapper.readValue(json, JobMeta.class);

        assertEquals("test-job", meta.getName());
        assertEquals("batch", meta.getMode());
        assertNull(meta.getParallelism());
    }

    @Test
    void testSerializeJobMetaWithParallelism() throws Exception {
        JobMeta meta = new JobMeta();
        meta.setName("test-job");
        meta.setMode("batch");
        meta.setParallelism(8);

        String json = mapper.writeValueAsString(meta);

        assertTrue(json.contains("\"name\":\"test-job\""));
        assertTrue(json.contains("\"mode\":\"batch\""));
        assertTrue(json.contains("\"parallelism\":8"));
    }
}