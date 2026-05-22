package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import com.etl.connector.jdbc.source.splitter.ChunkSplitter;
import com.etl.core.source.AbstractSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 分片枚举器
 * 继承 AbstractSplitEnumerator，在 start() 中执行分片计算
 *
 * <p>分片计算延迟到 enumerator 启动时执行，而非创建时预计算。
 * 这样可以在运行时动态获取数据范围，支持更灵活的分片策略。
 */
@Slf4j
public class JdbcSplitEnumerator extends AbstractSplitEnumerator<JdbcSplit, JdbcEnumCheckpoint> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSplitEnumerator(SplitEnumeratorContext<JdbcSplit> context, JdbcSourceConfig jdbcSourceConfig) {
        super(context);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 初始化");
    }

    public JdbcSplitEnumerator(SplitEnumeratorContext<JdbcSplit> context,
                               JdbcEnumCheckpoint checkpoint,
                               JdbcSourceConfig jdbcSourceConfig) {
        super(context, checkpoint);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.debug("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("启动 SplitEnumerator，并行度: {}", context.currentParallelism());

        JdbcSourceConfig config = this.jdbcSourceConfig;
        int parallelism = context.currentParallelism();
        SplitStrategy strategy = config.getSplitStrategy();

        // 1. 创建对应的 Splitter
        ChunkSplitter splitter = ChunkSplitter.create(strategy, config, parallelism);

        // 2. 生成分片
        List<JdbcSplit> splits = splitter.generateSplits();
        log.info("共生成 {} 个分片", splits.size());

        // 3. 添加到待分配列表（由父类处理分配逻辑）
        addPendingSplits(splits);
        log.info("JDBC SplitEnumerator 启动完成");
    }

    @Override
    public JdbcEnumCheckpoint snapshotState(long checkpointId) {
        List<JdbcSplit> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new JdbcEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("JDBC SplitEnumerator 关闭");
    }
}