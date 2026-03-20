package com.etl.source.localfile;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.function.Supplier;

/**
 * 本地文件 Source 实现
 * 支持通配符匹配文件，每个文件对应一个分片
 *
 * <p>使用组件：
 * <ul>
 *   <li>{@link LocalFileSplitEnumerator} - 分片枚举器</li>
 *   <li>{@link LocalFileSourceReader} - 源阅读器</li>
 *   <li>自定义序列化器 - 避免 Kryo 序列化问题</li>
 *   <li>直接输出 Flink Row 类型</li>
 * </ul>
 *
 * <p>字段名和类型从 source.schema 配置中获取
 */
@Slf4j
public class LocalFileSource extends AbstractSplitSource<LocalFileSplit, LocalFileEnumCheckpoint> {

    private final String pathPattern;
    private final String format;

    public LocalFileSource(SourceConfig config) {
        super(config);

        // 验证必要配置项
        pathPattern = config.getString("path");
        Preconditions.checkArgument(StringUtils.isNotBlank(pathPattern),"path is null");

        format = config.getString("format");
        Preconditions.checkArgument(StringUtils.isNotBlank(format),"format is null");

        log.info("创建 LocalFileSource: path={}, format={}", config.getString("path"), config.getString("format"));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint> createEnumerator(SplitEnumeratorContext<LocalFileSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, super.getConfig());
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint> restoreEnumerator(SplitEnumeratorContext<LocalFileSplit> enumContext,
                      LocalFileEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, checkpoint, super.getConfig());
    }

    @Override
    public SourceReader<Row, LocalFileSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        // 格式插件在 LocalFileSplitReader 内部动态加载
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, LocalFileSplit>>) () ->
                new LocalFileSplitReader(super.getConfig());

        // 创建 Reader
        return new LocalFileSourceReader(
                splitReaderSupplier,
                readerContext
        );
    }

    @Override
    public SimpleVersionedSerializer<LocalFileSplit> getSplitSerializer() {
        // 使用自定义序列化器，避免 Kryo 序列化 List<String> 的问题
        return new LocalFileSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LocalFileEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        // 使用自定义序列化器
        return new LocalFileEnumCheckpointSerializer();
    }
}