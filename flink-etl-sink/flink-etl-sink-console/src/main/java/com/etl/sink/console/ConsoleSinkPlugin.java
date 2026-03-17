package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * Console Sink 插件
 * 将数据输出到控制台
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class ConsoleSinkPlugin implements SinkPlugin {
    private static final long serialVersionUID = 1L;

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public RichSinkFunction<Row> createSink(SinkConfig config) {
        boolean showSubtask = config.getBoolean("showSubtask", true);
        log.info("创建 Console Sink, showSubtask={}", showSubtask);

        return new ConsoleSinkFunction(showSubtask);
    }

    /**
     * Console Sink Function
     * 使用 RichSinkFunction 获取 RuntimeContext，支持显示分片信息
     */
    private static class ConsoleSinkFunction extends RichSinkFunction<Row> {
        private static final long serialVersionUID = 1L;
        private final boolean showSubtask;

        // 缓存分片信息，避免每次 invoke 都调用
        private transient int subtaskIndex = -1;
        private transient int totalSubtasks = -1;

        public ConsoleSinkFunction(boolean showSubtask) {
            this.showSubtask = showSubtask;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在 open() 中获取分片信息，只执行一次
            RuntimeContext ctx = getRuntimeContext();
            TaskInfo taskInfo = ctx.getTaskInfo();
            this.subtaskIndex = taskInfo.getIndexOfThisSubtask() + 1;
            this.totalSubtasks = taskInfo.getNumberOfParallelSubtasks();
            log.info("ConsoleSinkFunction 初始化, subtask[{}/{}]", subtaskIndex, totalSubtasks);
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            if (showSubtask) {
                System.out.printf("[subtask-%d/%d] %s%n", subtaskIndex, totalSubtasks, value);
            } else {
                System.out.println(value);
            }
        }
    }
}