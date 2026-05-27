package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * 本地文件 Source 实现
 * 支持通配符匹配文件，每个文件对应一个分片
 *
 * <p>使用组件：
 * <ul>
 *   <li>{@link LocalFileSplitEnumerator} - 分片枚举器</li>
 *   <li>{@link LocalFileSourceReader} - 源阅读器</li>
 *   <li>默认序列化器 - 使用 JDK 原生序列化</li>
 *   <li>直接输出 Flink Row 类型</li>
 * </ul>
 *
 * <p>字段名和类型从 source.schema 配置中获取
 */
@Slf4j
public class LocalFileSource extends AbstractSplitSource<LocalFileSplit> {

    private final LocalFileSourceConfig localFileSourceConfig;

    public LocalFileSource(SourceConfig config) {
        super(config);
        this.localFileSourceConfig = LocalFileSourceConfig.fromSourceConfig(config, super.getDefaultBatchSize());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<LocalFileSplit, BaseEnumCheckpoint<LocalFileSplit>> createEnumerator(
            SplitEnumeratorContext<LocalFileSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, localFileSourceConfig);
    }

    @Override
    public SplitEnumerator<LocalFileSplit, BaseEnumCheckpoint<LocalFileSplit>> restoreEnumerator(
            SplitEnumeratorContext<LocalFileSplit> enumContext,
            BaseEnumCheckpoint<LocalFileSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, checkpoint, localFileSourceConfig);
    }

    @Override
    public SourceReader<Row, LocalFileSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<AbstractSplitReader<Row, LocalFileSplit>> splitReaderSupplier = LocalFileSplitReader::new;
        return new LocalFileSourceReader(
                splitReaderSupplier,
                readerContext
        );
    }

    @Override
    public SimpleVersionedSerializer<LocalFileSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<LocalFileSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
