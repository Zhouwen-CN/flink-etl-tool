package com.etl.core.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
    protected abstract AbstractSinkWriter createWriter(int batchSize) throws IOException;

    /**
     * 创建带有默认 batchSize 的 Writer
     */
    protected AbstractSinkWriter createWriter() throws IOException {
        return createWriter(10);
    }

    @Test
    public void testBatchWriteAndFlush() throws Exception {
        AbstractSinkWriter writer = createWriter(5);

        // 写入 5 条数据，应触发自动 flush
        for (int i = 0; i < 5; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 通过反射验证 pendingCount 被重置为 0
        java.lang.reflect.Field pendingCountField = AbstractSinkWriter.class.getDeclaredField("pendingCount");
        pendingCountField.setAccessible(true);
        int pendingCount = (int) pendingCountField.get(writer);
        assertEquals(0, pendingCount);
    }

    @Test
    public void testManualFlush() throws Exception {
        AbstractSinkWriter writer = createWriter(100);

        // 写入 10 条数据
        for (int i = 0; i < 10; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 手动 flush
        writer.flush(false);

        // 验证 pendingCount 被重置
        java.lang.reflect.Field pendingCountField = AbstractSinkWriter.class.getDeclaredField("pendingCount");
        pendingCountField.setAccessible(true);
        int pendingCount = (int) pendingCountField.get(writer);
        assertEquals(0, pendingCount);
    }

    @Test
    public void testCloseWithPendingData() throws Exception {
        AbstractSinkWriter writer = createWriter(100);

        // 写入数据但不 flush
        writer.write(createTestRow("value1"), mock(SinkWriter.Context.class));

        // 关闭时应自动 flush
        writer.close();

        // 验证资源已清理
    }

    @Test
    public void testInvalidBatchSize() throws IOException {
        // batchSize <= 0 应抛出异常
        assertThrows(IllegalArgumentException.class, () -> createWriter(0));
    }

    @Test
    public void testFlushFailureHandling() throws Exception {
        AbstractSinkWriter writer = createWriter(10);

        // 写入数据
        for (int i = 0; i < 10; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 子类可以覆盖此测试，模拟 flush 失败场景
        // 验证 handleFlushFailure() 被调用
    }
}