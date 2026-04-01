package com.etl.core.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * SinkWriter 抽象基类
 * 简化 Flink SinkWriter API 的实现，提供批量写入管理
 *
 * <p>该类特点：
 * <ul>
 *   <li>自动批量管理：维护待写入数据计数，达到 batchSize 时自动触发 flush</li>
 *   <li>立即初始化：构造函数中调用 open()，子类可覆盖进行初始化操作</li>
 *   <li>InitContext 访问：提供 helper 方法获取运行时信息（subtaskId、并行度、metrics）</li>
 *   <li>异常处理：flush 失败时调用 handleFlushFailure()，子类可覆盖进行自定义处理</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>{@link #writeRow(Row)}：写入单行数据的逻辑</li>
 *   <li>{@link #flushBatch()}：批量提交数据的逻辑</li>
 *   <li>{@link #cleanup()}：清理资源的逻辑</li>
 * </ul>
 *
 * <p>子类可选覆盖：
 * <ul>
 *   <li>{@link #open()}：初始化操作（如创建连接、打开文件）</li>
 *   <li>{@link #handleFlushFailure(IOException)}：处理 flush 失败（如记录失败数据）</li>
 * </ul>
 *
 * @param <ConfigT> Sink 配置类型
 */
public abstract class AbstractSinkWriter<ConfigT> implements SinkWriter<Row> {

    /** Writer 初始化上下文 */
    protected final Sink.InitContext context;

    /** Sink 配置对象 */
    protected final ConfigT config;

    /** 批量大小 */
    protected final int batchSize;

    /** 待写入数据计数 */
    protected int pendingCount = 0;

    /**
     * 构造函数
     *
     * <p>立即初始化模式：构造函数中调用 open()，确保 Writer 在创建后立即可用。
     *
     * @param context Writer 初始化上下文
     * @param config Sink 配置对象
     * @param batchSize 批量大小（must be > 0）
     * @throws IOException 如果 open() 初始化失败
     */
    public AbstractSinkWriter(Sink.InitContext context, ConfigT config, int batchSize) throws IOException {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }

        this.context = context;
        this.config = config;
        this.batchSize = batchSize;

        // 立即初始化
        open();
    }

    /**
     * 初始化操作
     * 子类可覆盖此方法进行初始化操作（如创建连接、打开文件）
     *
     * <p>默认实现为空操作。
     *
     * @throws IOException 如果初始化失败
     */
    protected void open() throws IOException {
        // 默认空操作，子类可覆盖
    }

    /**
     * 获取子任务 ID
     *
     * @return 当前 Writer 的子任务 ID（0-based）
     */
    protected int getSubtaskId() {
        return context.getSubtaskId();
    }

    /**
     * 获取并行子任务总数
     *
     * @return 并行任务总数
     */
    protected int getNumberOfParallelSubtasks() {
        return context.getNumberOfParallelSubtasks();
    }

    /**
     * 获取 Metric Group
     * 子类可通过此方法注册自定义指标
     *
     * @return Flink SinkWriterMetricGroup
     */
    protected SinkWriterMetricGroup getMetricGroup() {
        return context.metricGroup();
    }

    /**
     * 写入数据
     * 自动批量管理：当待写入数据达到 batchSize 时自动触发 flush
     *
     * @param row 数据行
     * @param context 写入上下文（包含 watermark 和 timestamp 信息）
     * @throws IOException 如果写入或 flush 失败
     * @throws InterruptedException 如果写入被中断
     */
    @Override
    public final void write(Row row, Context context) throws IOException, InterruptedException {
        writeRow(row);
        pendingCount++;

        // 达到批量大小时自动 flush
        if (pendingCount >= batchSize) {
            flush(false);
        }
    }

    /**
     * 写入单行数据
     * 子类实现此方法定义写入逻辑（如缓存到内存队列、写入文件缓冲区）
     *
     * <p>注意：此方法只负责写入单行，不负责提交。
     *
     * @param row 数据行
     * @throws IOException 如果写入失败
     */
    protected abstract void writeRow(Row row) throws IOException;

    /**
     * 提交批量数据
     * 清空待写入数据计数，调用 flushBatch() 提交数据
     *
     * @param endOfInput 是否为输入结束时的 flush
     * @throws IOException 如果 flush 失败
     * @throws InterruptedException 如果 flush 被中断
     */
    @Override
    public final void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (pendingCount == 0) {
            return;
        }

        try {
            flushBatch();
            pendingCount = 0;
        } catch (IOException e) {
            handleFlushFailure(e);
            throw e;
        }
    }

    /**
     * 批量提交数据
     * 子类实现此方法定义批量提交逻辑（如提交到数据库、写入文件）
     *
     * @throws IOException 如果提交失败
     */
    protected abstract void flushBatch() throws IOException;

    /**
     * 处理 flush 失败
     * 子类可覆盖此方法进行自定义处理（如记录失败数据、发送告警）
     *
     * <p>默认实现为空操作。
     *
     * @param e flush 失败异常
     */
    protected void handleFlushFailure(IOException e) {
        // 默认空操作，子类可覆盖
    }

    /**
     * 关闭 Writer
     * 先执行 flush 提交剩余数据，然后调用 cleanup 清理资源
     *
     * @throws IOException 如果 flush 或 cleanup 失败
     */
    @Override
    public final void close() throws IOException {
        try {
            flush(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while flushing during close", e);
        } finally {
            cleanup();
        }
    }

    /**
     * 清理资源
     * 子类实现此方法定义资源清理逻辑（如关闭连接、关闭文件）
     *
     * @throws IOException 如果清理失败
     */
    protected abstract void cleanup() throws IOException;
}