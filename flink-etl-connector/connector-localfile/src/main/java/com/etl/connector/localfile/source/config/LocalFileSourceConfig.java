package com.etl.connector.localfile.source.config;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.connector.localfile.source.format.FileFormatPlugin;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ServiceLoader;

/**
 * LocalFile Source 配置
 * 用于传递分片所需的所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
@Slf4j
public class LocalFileSourceConfig implements Serializable {
    /** 文件路径模式（支持通配符） */
    private final String pathPattern;
    /** 文件格式（csv、excel 等） */
    private final String format;
    /** 是否递归扫描目录 */
    private final boolean recursive;
    /** 批大小，默认 1000 */
    private final Integer batchSize;
    /**
     * schema
     */
    private final EtlSchema schema;
    /**
     * 编码
     */
    private final String encoding;
    /**
     * 分隔符
     */
    private final String delimiter;
    /**
     * 是否跳过首行
     */
    private final boolean skipHeader;
    /**
     * 格式插件
     */
    private final FileFormatPlugin formatPlugin;

    /**
     * 从 SourceConfig 创建 LocalFileSourceConfig，在此完成所有参数校验
     */
    public static LocalFileSourceConfig fromSourceConfig(SourceConfig config, int defaultBatchSize) {
        // 路径
        String pathPattern = config.getString("path");
        Preconditions.checkArgument(StringUtils.isNotBlank(pathPattern), "path is null");

        // 格式
        String format = config.getString("format");
        Preconditions.checkArgument(StringUtils.isNotBlank(format), "format is null");

        // 加载格式插件
        FileFormatPlugin formatPlugin = loadFormatPlugin(format);

        // 批次大小
        Integer batchSize = config.getInteger("batchSize", defaultBatchSize);
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        // 是否递归
        boolean recursive = config.getBoolean("recursive", false);

        // schema
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // CSV 格式不支持复杂类型，校验 schema
        if (schema.hasComplexType()) {
            throw new SchemaConfigException("LocalFileSource CSV 格式只支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }

        // 编码
        String encoding = config.getString("encoding", "utf-8");

        // 分隔符
        String delimiter = config.getString("delimiter", ",");

        // 是否跳过首行
        boolean skipHeader = config.getBoolean("skipHeader", true);

        log.info("创建 LocalFileSource: pathPattern={}, format={}, recursive={}, batchSize={}, encoding={}, delimiter={}, skipHeader={}",
                pathPattern,
                format,
                recursive,
                batchSize,
                encoding,
                delimiter,
                skipHeader
        );

        return LocalFileSourceConfig.builder()
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
    }

    /**
     * 加载格式插件
     */
    private static FileFormatPlugin loadFormatPlugin(String format) {
        ServiceLoader<FileFormatPlugin> loader = ServiceLoader.load(FileFormatPlugin.class);
        for (FileFormatPlugin plugin : loader) {
            if (plugin.getType().equalsIgnoreCase(format)) {
                log.info("加载格式插件: {}", plugin.getClass().getName());
                return plugin;
            }
        }
        throw new RuntimeException("未找到格式插件: " + format);
    }
}