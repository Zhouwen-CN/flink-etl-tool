package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import com.etl.core.schema.metadata.MetadataManager;
import com.etl.core.sink.AbstractSinkWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * JDBC Sink Writer 实现
 * 连接生命周期委托给 JdbcOutputFormat（含断连自愈）
 */
@Slf4j
public class JdbcSinkWriter extends AbstractSinkWriter {

    private final JdbcSinkConfig config;
    private transient JdbcOutputFormat outputFormat;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) {
        super(context);
        this.config = config;

        log.info("JDBC Sink Writer 初始化: url={}, mode={}, subtaskId={}",
                config.getUrl(), config.getMode(), context.getSubtaskId());
    }

    @Override
    public void write(Row row) throws IOException, InterruptedException {
        row = MetadataManager.removeMetadata(row);
        // 首次写入时缓存列名
        if (outputFormat == null) {
            String[] columns = row.getFieldNames(true).toArray(new String[0]);
            log.debug("JDBC Sink 写入字段: {}", Arrays.toString(columns));

            // 延迟创建 OutputFormat（等 columns 确定后再 build），open 时建连
            try {
                this.outputFormat = new JdbcOutputFormatBuilder(config, columns).build();
                this.outputFormat.open();
            } catch (SQLException | RuntimeException e) {
                throw new IOException("Failed to open JDBC output", e);
            }
        }

        try {
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
    public void close() {
        // OutputFormat 关闭时会一并释放连接（含重连后的新连接）
        IOUtils.closeQuietly(outputFormat);
        log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
    }
}
