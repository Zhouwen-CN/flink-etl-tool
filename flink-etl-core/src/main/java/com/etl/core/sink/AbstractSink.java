package com.etl.core.sink;

import com.etl.core.config.SinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Sink 抽象基类
 * 简化 Flink Sink API 的实现
 *
 * <p>该类特点：
 * <ul>
 *   <li>接收 SinkConfig 配置对象（参数校验由具体实现类负责）</li>
 *   <li>实现 at-least-once 语义（通过 flush() 保证数据写入）</li>
 *   <li>不保存状态，故障恢复后从上次 checkpoint 重放数据</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>构造函数中的参数校验和配置对象构建</li>
 *   <li>{@link #createWriter(InitContext)} 创建具体的 Writer</li>
 * </ul>
 */
public abstract class AbstractSink implements Sink<Row> {

    /** 原始配置对象 */
    protected final SinkConfig config;

    /**
     * 构造函数
     *
     * @param config Sink 配置对象
     *               参数校验在具体实现类的构造函数中进行
     */
    public AbstractSink(SinkConfig config) {
        this.config = config;
    }

    /**
     * 创建 SinkWriter
     * 子类实现此方法返回具体的 Writer 实例
     *
     * @param context Writer 初始化上下文
     * @return SinkWriter 实例
     */
    @Override
    public abstract SinkWriter<Row> createWriter(InitContext context) throws IOException;


    /**
     * 所有 sink 默认的 batchSize，有些 sink 可能不需要
     *
     * @return 批次大小
     */
    public int getDefaultBatchSize() {
        return 100;
    }
}