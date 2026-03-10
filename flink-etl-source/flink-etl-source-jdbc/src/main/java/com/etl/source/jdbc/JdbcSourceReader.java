package com.etl.source.jdbc;

import com.etl.core.source.RangeSplit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * JDBC Source Reader
 * 负责执行 SQL 查询并读取数据
 */
public class JdbcSourceReader implements SourceReader<Row, RangeSplit> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSourceReader.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;
    private final SourceReaderContext context;

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private RangeSplit currentSplit;

    public JdbcSourceReader(String url, String username, String password,
                            String table, String sql, String splitColumn,
                            Integer fetchSize, Integer queryTimeout,
                            JdbcDialect dialect, SourceReaderContext context) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.sql = sql;
        this.splitColumn = splitColumn;
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeout;
        this.dialect = dialect;
        this.context = context;
    }

    @Override
    public void start() {
        logger.info("JDBC Source Reader 启动");
        try {
            // 加载驱动
            Class.forName(dialect.getDriverClassName());
            // 创建连接
            connection = DriverManager.getConnection(url, username, password);
            logger.info("数据库连接成功");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC 驱动加载失败: " + e.getMessage(), e);
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接失败: " + e.getMessage(), e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Row> output) throws Exception {
        if (resultSet == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (resultSet.next()) {
            try {
                // 创建 Row 并输出
                Row row = dialect.createRow(resultSet);
                output.collect(row);
                return InputStatus.MORE_AVAILABLE;
            } catch (SQLException e) {
                throw new RuntimeException("数据读取失败: " + e.getMessage(), e);
            }
        } else {
            // 当前分片读取完毕
            logger.info("分片 {} 读取完毕", currentSplit.splitId());
            closeCurrentSplit();
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public void addSplits(List<RangeSplit> splits) {
        if (!splits.isEmpty()) {
            this.currentSplit = splits.get(0);
            logger.info("接收分片: {}", currentSplit.splitId());
            executeQuery();
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        logger.info("通知无更多分片");
    }

    @Override
    public void close() throws Exception {
        logger.info("关闭 JDBC Source Reader");
        closeCurrentSplit();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void executeQuery() {
        String querySql = dialect.buildSplitQuery(table, sql, splitColumn,
                currentSplit.getStart(), currentSplit.getEnd());

        try {
            statement = connection.createStatement();
            if (fetchSize != null) {
                statement.setFetchSize(fetchSize);
            }
            if (queryTimeout != null) {
                statement.setQueryTimeout(queryTimeout);
            }

            logger.info("执行查询 SQL: {}", querySql);
            resultSet = statement.executeQuery(querySql);
        } catch (SQLException e) {
            throw new RuntimeException("查询执行失败: " + e.getMessage(), e);
        }
    }

    private void closeCurrentSplit() {
        try {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
            }
            if (statement != null) {
                statement.close();
                statement = null;
            }
        } catch (SQLException e) {
            logger.error("关闭资源失败", e);
        }
    }
}