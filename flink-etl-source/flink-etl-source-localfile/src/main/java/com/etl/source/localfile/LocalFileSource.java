package com.etl.source.localfile;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SourceConfigException;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
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
 *   <li>自定义序列化器 - 避免 Kryo 序列化问题</li>
 *   <li>直接输出 Flink Row 类型</li>
 * </ul>
 *
 * <p>字段名通过 Split 传递，支持分布式环境。
 * <p>格式插件动态加载，避免序列化问题。
 */
@Slf4j
public class LocalFileSource extends AbstractSplitSource<LocalFileSplit, LocalFileEnumCheckpoint> {

    private final SourceConfig config;
    private final String format;

    public LocalFileSource(SourceConfig config) {
        super(config);
        this.config = config;

        // 验证必要配置项
        validateConfig(config);

        this.format = config.getString("format");

        log.info("创建 LocalFileSource: path={}, format={}",
                config.getString("path"), format);
    }

    /**
     * 验证配置项
     *
     * @param config 配置
     */
    private void validateConfig(SourceConfig config) {
        String path = config.getString("path");
        if (path == null || path.trim().isEmpty()) {
            throw new SourceConfigException("path 配置项不能为空");
        }

        String format = config.getString("format");
        if (format == null || format.trim().isEmpty()) {
            throw new SourceConfigException("format 配置项不能为空");
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<LocalFileSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, config, format);
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<LocalFileSplit> enumContext,
                      LocalFileEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, checkpoint, config, format);
    }

    @Override
    public SourceReader<Row, LocalFileSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        // 字段名从 Split 中获取，支持分布式环境
        // 格式插件动态加载，避免序列化问题
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, LocalFileSplit>>) () ->
                new LocalFileSplitReader(config, format);

        // 创建 Reader
        return new LocalFileSourceReader(
                splitReaderSupplier,
                new Configuration(),
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