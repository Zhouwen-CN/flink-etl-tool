package com.etl.connector.localfile.source.format;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.io.Serializable;

/**
 * 文件格式解析插件接口
 * SPI 接口，支持扩展不同的文件格式解析器
 *
 * <p>实现类需要：
 * <ul>
 *   <li>添加 {@link com.google.auto.service.AutoService} 注解</li>
 *   <li>实现所有方法</li>
 * </ul>
 *
 * <p>字段名和类型从 source.schema 配置中获取
 */
public interface FileFormatPlugin extends Serializable {

    /**
     * 获取格式类型标识
     * 用于配置中的 format 字段匹配
     *
     * @return 格式类型，如 "csv"、"excel"
     */
    String getType();

    /**
     * 解析文件内容，返回 Row 迭代器
     *
     * @param config 配置
     * @param inputStream 文件输入流
     *                    调用方负责打开，实现方负责在迭代完成后关闭
     * @return Row 迭代器
     */
    Iterable<Row> parse(LocalFileSourceConfig config, InputStream inputStream);
}
