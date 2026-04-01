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
        super(context, true, Integer.MAX_VALUE);  // 不触发批量 flush，config 参数传入 Boolean.TRUE
        this.showSubtask = showSubtask;
        this.subtaskId = getSubtaskId();
        this.totalSubtasks = getNumberOfParallelSubtasks();

        log.info("Console Sink Writer 初始化, subtask[{}/{}]", subtaskId + 1, totalSubtasks);
    }

    @Override
    protected void writeRow(Row row) throws IOException {
        // 直接输出，不缓冲
        if (showSubtask) {
            System.out.printf("[subtask-%d/%d] %s%n", subtaskId + 1, totalSubtasks, row);
        } else {
            System.out.println(row);
        }
    }

    @Override
    protected void flushBatch() throws IOException {
        // Console 直接输出，无需批量提交
    }

    @Override
    protected void cleanup() throws IOException {
        // 无需清理资源
        log.debug("Console Sink Writer 关闭");
    }
}