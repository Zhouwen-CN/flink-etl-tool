package com.etl.core.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * AbstractSinkWriter 测试基类
 */
public abstract class AbstractSinkWriterTest {

    protected Sink.InitContext mockContext;
    protected SinkWriterMetricGroup mockMetricGroup;

    @BeforeEach
    public void setUp() {
        mockContext = mock(Sink.InitContext.class);
        mockMetricGroup = mock(SinkWriterMetricGroup.class);

        when(mockContext.getSubtaskId()).thenReturn(0);
        when(mockContext.getNumberOfParallelSubtasks()).thenReturn(1);
        when(mockContext.metricGroup()).thenReturn(mockMetricGroup);
    }

    /**
     * 创建测试用的 Row 对象
     */
    protected Row createTestRow(String... values) {
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }

    /**
     * 创建具体的 Writer 实例
     */
    protected abstract AbstractSinkWriter createWriter() throws IOException;

    @Test
    public void testContextAccess() throws Exception {
        AbstractSinkWriter writer = createWriter();

        assertNotNull(writer.context);
        assertEquals(0, writer.context.getSubtaskId());
        assertEquals(1, writer.context.getNumberOfParallelSubtasks());
    }
}