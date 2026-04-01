package com.etl.sink.console;

import com.etl.core.sink.AbstractSinkWriter;
import com.etl.core.sink.AbstractSinkWriterTest;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * Console Sink Writer 测试
 */
public class ConsoleSinkWriterTest extends AbstractSinkWriterTest {

    @Override
    protected AbstractSinkWriter<Boolean> createWriter() throws IOException {
        return new ConsoleSinkWriter(mockContext);
    }

    @org.junit.jupiter.api.Test
    public void testWriteOutput() throws Exception {
        ConsoleSinkWriter writer = new ConsoleSinkWriter(mockContext);

        Row row = createTestRow("value1", "value2");
        writer.write(row, mock(SinkWriter.Context.class));

        // 验证输出到控制台（可以通过 System.out 捕获验证）
    }

    @org.junit.jupiter.api.Test
    public void testFlushDoesNothing() throws Exception {
        ConsoleSinkWriter writer = new ConsoleSinkWriter(mockContext);

        writer.flush(false);
        // 无异常即为成功
    }

    @org.junit.jupiter.api.Test
    public void testCloseDoesNothing() throws Exception {
        ConsoleSinkWriter writer = new ConsoleSinkWriter(mockContext);

        writer.close();
        // 无异常即为成功
    }
}