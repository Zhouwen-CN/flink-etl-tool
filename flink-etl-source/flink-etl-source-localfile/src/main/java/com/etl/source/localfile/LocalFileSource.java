package com.etl.source.localfile;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.localfile.config.LocalFileSourceConfig;
import com.etl.source.localfile.format.FileFormatPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ServiceLoader;
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

        // 路径
        String pathPattern = config.getString("path");
        Preconditions.checkArgument(StringUtils.isNotBlank(pathPattern), "path is null");

        // 格式
        String format = config.getString("format");
        Preconditions.checkArgument(StringUtils.isNotBlank(format), "format is null");

        // 加载格式插件
        FileFormatPlugin formatPlugin = loadFormatPlugin(format);

        // 批次大小
        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        // 是否递归
        boolean recursive = config.getBoolean("recursive", false);

        // schema
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // 编码
        String encoding = config.getString("encoding", "utf-8");

        // 分隔符
        String delimiter = config.getString("delimiter", ",");

        // 是否跳过首行
        boolean skipHeader = config.getBoolean("skipHeader", true);

        // 封装成配置对象
        this.localFileSourceConfig = LocalFileSourceConfig.builder()
                .pathPattern(pathPattern)
                .format(format)
                .recursive(recursive)
                .batchSize(batchSize)
                .schema(schema)
                .encoding(encoding)
                .delimiter(delimiter)
                .skipHeader(skipHeader)
                .formatPlugin(formatPlugin)
                .build();

        log.info("创建 LocalFileSource: {}", this.localFileSourceConfig);
    }

    /**
     * 加载格式插件
     */
    private FileFormatPlugin loadFormatPlugin(String format) {
        ServiceLoader<FileFormatPlugin> loader = ServiceLoader.load(FileFormatPlugin.class);
        for (FileFormatPlugin plugin : loader) {
            if (plugin.getType().equalsIgnoreCase(format)) {
                log.info("加载格式插件: {}", plugin.getClass().getName());
                return plugin;
            }
        }
        throw new RuntimeException("未找到格式插件: " + format);
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
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, LocalFileSplit>>) () ->
                new LocalFileSplitReader(localFileSourceConfig);

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