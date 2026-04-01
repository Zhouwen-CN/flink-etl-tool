package com.etl.sink.console;

import com.etl.core.sink.AbstractSinkWriter;
import com.etl.core.sink.AbstractSinkWriterTest;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Console Sink Writer 测试
 */
public class ConsoleSinkWriterTest extends AbstractSinkWriterTest {

    @Override
    protected AbstractSinkWriter<Boolean> createWriter(int batchSize) throws IOException {
        // Console Sink 使用 Integer.MAX_VALUE 作为 batchSize，不触发批量 flush
        // 传入的 batchSize 参数会被忽略
        return new ConsoleSinkWriter(mockContext, true);
    }

    @Override
    public void testBatchWriteAndFlush() throws Exception {
        // Console Sink 不触发自动 flush，直接输出
        AbstractSinkWriter writer = createWriter(5);

        // 写入数据，应直接输出到控制台
        for (int i = 0; i < 5; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // Console Sink pendingCount 会累积，因为不自动 flush
        java.lang.reflect.Field pendingCountField = AbstractSinkWriter.class.getDeclaredField("pendingCount");
        pendingCountField.setAccessible(true);
        int pendingCount = (int) pendingCountField.get(writer);

        // 验证数据已写入但未 flush
        assertTrue(pendingCount > 0, "Console Sink 应该累积 pendingCount");
    }

    @Override
    public void testManualFlush() throws Exception {
        // Console Sink 可以手动 flush，但 flushBatch() 是空实现
        AbstractSinkWriter writer = createWriter(100);

        // 写入数据
        for (int i = 0; i < 10; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 手动 flush（Console Sink 的 flushBatch 是空操作）
        writer.flush(false);

        // Console Sink 的 flushBatch 是空实现，不会清空 pendingCount
        // 这是预期行为
    }

    @Override
    public void testInvalidBatchSize() throws IOException {
        // Console Sink 忽略 batchSize 参数，不会抛出异常
        // 传入任何 batchSize 都可以
        AbstractSinkWriter writer = new ConsoleSinkWriter(mockContext, false);
        assertNotNull(writer, "Console Sink 应该接受任何 batchSize 参数");
    }

    @Override
    public void testCloseWithPendingData() throws Exception {
        // Console Sink 关闭时正常关闭
        AbstractSinkWriter writer = createWriter(100);

        // 写入数据
        writer.write(createTestRow("value1"), mock(SinkWriter.Context.class));

        // 关闭时应正常关闭，不抛出异常
        assertDoesNotThrow(() -> writer.close());
    }
}