package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import com.etl.core.sink.AbstractSinkWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * JDBC Sink Writer 实现
 * 简化为调用 OutputFormat
 */
@Slf4j
public class JdbcSinkWriter extends AbstractSinkWriter<JdbcSinkConfig> {

    private final transient Connection connection;
    private transient JdbcOutputFormat outputFormat;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config);

        // 初始化数据库连接
        try {
            connection = DriverManager.getConnection(
                    config.getUrl(),
                    config.getUsername(),
                    config.getPassword()
            );
            connection.setAutoCommit(false);

            log.info("JDBC Sink Writer 已连接: url={}, mode={}, subtaskId={}",
                    config.getUrl(), config.getMode(), context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        try {
            // 首次写入时缓存列名
            if (outputFormat == null) {
                String[] columns = row.getFieldNames(true).toArray(new String[0]);

                log.debug("JDBC Sink 写入字段（已过滤隐藏字段）: {}", Arrays.toString(columns));

                // 延迟创建 OutputFormat（等 columns 确定后再 build）
                this.outputFormat = new JdbcOutputFormatBuilder(config, connection, columns).build();
                this.outputFormat.open();
            }

            outputFormat.writeRecord(row);
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (outputFormat != null) {
            outputFormat.flush();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // 提交剩余数据并关闭 OutputFormat
            if (outputFormat != null) {
                outputFormat.close();
            }

            // 关闭数据库连接
            if (connection != null) {
                connection.close();
            }
            log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to cleanup JDBC resources", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing", e);
        }
    }
}
