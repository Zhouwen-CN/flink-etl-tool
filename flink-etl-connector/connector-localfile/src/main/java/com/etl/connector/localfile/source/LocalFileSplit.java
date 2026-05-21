package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.core.source.BaseSourceSplit;
import lombok.Getter;

import java.io.File;

/**
 * 文件分片
 * 一个文件对应一个分片
 *
 * <p>字段名和类型从 source.schema 配置中获取，无需从文件推断
 */
@Getter
public class LocalFileSplit implements BaseSourceSplit {

    private static final long serialVersionUID = 1L;

    /** 分片 ID = 文件路径 */
    private final String splitId;

    /** 文件绝对路径 */
    private final String filePath;

    /** 文件名 */
    private final String fileName;

    /** 配置信息 */
    private final LocalFileSourceConfig config;

    /**
     * 构造函数
     *
     * @param filePath 文件绝对路径
     * @param config   配置信息
     */
    public LocalFileSplit(String filePath, LocalFileSourceConfig config) {
        this.filePath = filePath;
        this.fileName = new File(filePath).getName();
        this.splitId = filePath;
        this.config = config;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
