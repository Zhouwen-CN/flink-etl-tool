package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.core.schema.convert.SqlTypeConverter;
import com.etl.core.schema.convert.TypeConverter;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * JDBC 分片读取器
 * 实现阻塞式数据读取，配合 BaseSourceReader 使用
 *
 * <p>设计说明：
 * <ul>
 *   <li>每个分片创建独立的数据库连接</li>
 *   <li>支持分批读取，每批最多 batchSize 条记录</li>
 *   <li>直接返回 Flink Row 类型，无需额外包装</li>
 *   <li>连接参数、batchSize、queryTimeout 全部从 JdbcSourceConfig 获取，Split 只携带 splitId 和 querySql</li>
 * </ul>
 */
@Slf4j
public class JdbcSplitReader extends AbstractSplitReader<Row, JdbcSplit> {

    private final JdbcSourceConfig jdbcSourceConfig;

    private final Set<String> finishedSplits = new HashSet<>();

    // 当前分片读取状态
    private JdbcSplit currentSplit;
    private Connection currentConnection;
    private Statement currentStatement;
    private ResultSet currentResultSet;
    private boolean hasNextRecord;

    private String[] columnNames;
    private TypeInformation<?>[] flinkTypes;

    public JdbcSplitReader(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            JdbcSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            // 开始新分片
            startNewSplit(split);
        }

        // 读取一批数据
        return fetchBatch();
    }

    /**
     * 开始读取新分片
     */
    private void startNewSplit(JdbcSplit split) throws IOException {
        log.info("开始读取分片: {}", split.splitId());
        try {
            // 创建连接（凭证来自 JdbcSourceConfig，不走 split 序列化）
            currentConnection = JdbcUtil.getConnection(
                    jdbcSourceConfig.getUrl(), jdbcSourceConfig.getUsername(), jdbcSourceConfig.getPassword(), jdbcSourceConfig.getDialect().driverClassName());
            currentStatement = currentConnection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );

            // 设置 fetchSize 和 queryTimeout（全部来自 JdbcSourceConfig）
            Integer queryTimeout = jdbcSourceConfig.getQueryTimeout();
            currentStatement.setFetchSize(jdbcSourceConfig.getBatchSize());
            if (queryTimeout != null) {
                currentStatement.setQueryTimeout(queryTimeout);
            }

            // 使用分片中预先构建好的查询 SQL
            String querySql = split.getQuerySql();

            log.debug("执行查询: {}", querySql);

            // 执行查询
            currentResultSet = currentStatement.executeQuery(querySql);
            hasNextRecord = currentResultSet.next();
            currentSplit = split;
        } catch (SQLException e) {
            // 出错时关闭资源
            closeCurrentSplit();
            throw new IOException("读取分片失败: " + split.splitId(), e);
        }
    }

    /**
     * 读取一批数据
     */
    private RecordsWithSplitIds<Row> fetchBatch() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int recordsInBatch = 0;
            ResultSetMetaData metaData = currentResultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 计算列元数据
            if (columnNames == null || flinkTypes == null) {
                columnNames = new String[columnCount];
                flinkTypes = new TypeInformation<?>[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    columnNames[i] = metaData.getColumnLabel(i + 1);
                    int sqlType = metaData.getColumnType(i + 1);
                    flinkTypes[i] = SqlTypeConverter.fromSqlType(sqlType);
                }
            }

            int batchSize = jdbcSourceConfig.getBatchSize();

            // 读取一批记录
            while (hasNextRecord && recordsInBatch < batchSize) {
                Row row = new Row(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    Object rawValue = currentResultSet.getObject(i + 1);
                    Object convertedValue = TypeConverter.convertFromValue(rawValue, columnNames[i], flinkTypes[i]);
                    row.setField(i, convertedValue);
                }

                builder.add(currentSplit.splitId(), row);
                recordsInBatch++;

                // 读取下一条记录
                hasNextRecord = currentResultSet.next();
            }

            // 如果没有更多记录，标记分片完成
            if (!hasNextRecord) {
                finishedSplits.add(currentSplit.splitId());
                log.debug("分片 {} 读取完成", currentSplit.splitId());

                // 关闭资源
                closeCurrentSplit();
            }
        } catch (SQLException e) {
            closeCurrentSplit();
            throw new IOException("读取分片失败: " + currentSplit.splitId(), e);
        }

        return builder.build();
    }

    /**
     * 关闭当前分片的资源
     */
    private void closeCurrentSplit() {
        IOUtils.closeQuietly(currentResultSet);
        IOUtils.closeQuietly(currentStatement);
        IOUtils.closeQuietly(currentConnection);

        currentResultSet = null;
        currentStatement = null;
        currentConnection = null;
        currentSplit = null;
        hasNextRecord = false;
        columnNames = null;
        flinkTypes = null;
    }

    @Override
    public void close() throws Exception {
        closeCurrentSplit();
        log.info("JdbcSplitReader 关闭");
    }
}
