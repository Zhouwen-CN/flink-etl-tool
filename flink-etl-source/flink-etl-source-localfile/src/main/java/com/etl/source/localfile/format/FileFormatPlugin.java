package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.util.List;

/**
 * 文件格式解析插件接口
 * SPI 接口，支持扩展不同的文件格式解析器
 *
 * <p>实现类需要：
 * <ul>
 *   <li>添加 {@link com.google.auto.service.AutoService} 注解</li>
 *   <li>实现所有方法</li>
 * </ul>
 */
public interface FileFormatPlugin {

    /**
     * 获取格式类型标识
     * 用于配置中的 format 字段匹配
     *
     * @return 格式类型，如 "csv"、"excel"
     */
    String getType();

    /**
     * 获取字段名列表
     * header=true 时从文件头解析，header=false 时从配置获取
     *
     * @param config 配置
     * @param firstFile 第一个文件的输入流（用于解析文件头）
     *                  调用方负责打开和关闭此流
     * @return 字段名列表
     */
    List<String> resolveFields(SourceConfig config, InputStream firstFile);

    /**
     * 解析文件内容，返回 Row 迭代器
     *
     * @param config 配置
     * @param inputStream 文件输入流
     *                    调用方负责打开，实现方负责在迭代完成后关闭
     * @param fields 字段名列表
     * @return Row 迭代器
     */
    Iterable<Row> parse(SourceConfig config, InputStream inputStream, List<String> fields);
}