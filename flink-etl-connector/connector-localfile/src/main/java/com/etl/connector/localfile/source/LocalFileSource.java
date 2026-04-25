package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
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
public class LocalFileSource extends AbstractSplitSource<LocalFileSplit, LocalFileEnumCheckpoint> {

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
    public SplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<LocalFileSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, localFileSourceConfig);
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<LocalFileSplit> enumContext,
            LocalFileEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, checkpoint, localFileSourceConfig);
    }

    @Override
    public SourceReader<Row, LocalFileSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        Supplier<BaseSplitReader<Row, LocalFileSplit>> splitReaderSupplier = LocalFileSplitReader::new;

        // 创建 Reader
        return new LocalFileSourceReader(
                splitReaderSupplier,
                readerContext
        );
    }

    @Override
    public SimpleVersionedSerializer<LocalFileSplit> getSplitSerializer() {
        // 使用默认序列化器，基于 JDK 原生序列化
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<LocalFileEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        // 使用默认序列化器，基于 JDK 原生序列化
        return new DefaultCheckpointSerializer<>();
    }
}