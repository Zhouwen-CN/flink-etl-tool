package com.etl.core.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * SinkWriter 抽象基类
 * 简化 Flink SinkWriter API 的实现，提供最小化抽象
 *
 * <p>该类特点：
 * <ul>
 *   <li>InitContext 访问：通过 protected context 字段，子类可直接访问运行时信息</li>
 *   <li>配置管理：通过 protected config 字段，统一管理 Sink 配置参数</li>
 *   <li>最小化抽象：只定义 write 和 flush 抽象方法，具体行为完全由子类实现</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>{@link #write(Row, Context)}：写入数据的逻辑（自行决定是否批量）</li>
 *   <li>{@link #flush(boolean)}：提交数据的逻辑（如批量提交）</li>
 *   <li>{@link SinkWriter#close()}：清理资源的逻辑（如关闭连接）</li>
 * </ul>
 *
 * @param <ConfigT> Sink 配置类型
 */
public abstract class AbstractSinkWriter<ConfigT> implements SinkWriter<Row> {

    /** Writer 初始化上下文 */
    protected final Sink.InitContext context;

    /** Sink 配置对象 */
    protected final ConfigT config;

    /**
     * 构造函数
     *
     * @param context Writer 初始化上下文
     * @param config Sink 配置对象
     */
    public AbstractSinkWriter(Sink.InitContext context, ConfigT config) {
        this.context = context;
        this.config = config;
    }

    /**
     * 写入数据
     * 子类实现此方法定义写入逻辑，自行决定是否需要批量管理
     *
     * @param row 数据行
     * @param context 写入上下文
     * @throws IOException 如果写入失败
     * @throws InterruptedException 如果写入被中断
     */
    @Override
    public abstract void write(Row row, Context context) throws IOException, InterruptedException;

    /**
     * 提交数据
     * 子类实现此方法定义提交逻辑
     *
     * @param endOfInput 是否为输入结束时的 flush
     * @throws IOException 如果提交失败
     * @throws InterruptedException 如果提交被中断
     */
    @Override
    public abstract void flush(boolean endOfInput) throws IOException, InterruptedException;
}