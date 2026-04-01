package com.etl.sink.console;

import com.etl.core.sink.AbstractSinkWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Console Sink Writer 实现
 * 直接将数据输出到控制台
 */
@Slf4j
public class ConsoleSinkWriter extends AbstractSinkWriter<Boolean> {

    private final boolean showSubtask;
    private final int subtaskId;
    private final int totalSubtasks;

    public ConsoleSinkWriter(Sink.InitContext context, boolean showSubtask) throws IOException {
        super(context, true);  // config 参数传入 Boolean.TRUE
        this.showSubtask = showSubtask;
        this.subtaskId = context.getSubtaskId();
        this.totalSubtasks = context.getNumberOfParallelSubtasks();

        log.info("Console Sink Writer 初始化, subtask[{}/{}]", subtaskId + 1, totalSubtasks);
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        if (showSubtask) {
            System.out.printf("[subtask-%d/%d] %s%n", subtaskId + 1, totalSubtasks, row);
        } else {
            System.out.println(row);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // Console Sink 不需要批量提交，空实现
    }

    @Override
    public void close() throws IOException {
        log.debug("Console Sink Writer 关闭");
    }
}