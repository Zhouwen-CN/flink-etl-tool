package com.etl.source.localfile.config;

import com.etl.core.schema.EtlSchema;
import com.etl.source.localfile.format.FileFormatPlugin;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * LocalFile Source 配置
 * 用于传递分片所需的所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
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
}