package com.etl.sink.jdbc;

import com.etl.core.dialect.MySQLDialect;
import com.etl.core.dialect.WriteMode;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;

import static org.mockito.Mockito.*;

/**
 * JdbcSinkWriter 执行测试
 * 测试 initStatement() 方法的 mode 判断逻辑
 */
public class JdbcSinkWriterTest {

    @Mock
    protected Sink.InitContext mockContext;

    @Mock
    protected SinkWriterMetricGroup mockMetricGroup;

    @Mock
    protected Connection mockConnection;

    @Mock
    protected PreparedStatement mockStatement;

    @BeforeEach
    public void setUp() {
        mockContext = mock(Sink.InitContext.class);
        mockMetricGroup = mock(SinkWriterMetricGroup.class);

        when(mockContext.getSubtaskId()).thenReturn(0);
        when(mockContext.getNumberOfParallelSubtasks()).thenReturn(1);
        when(mockContext.metricGroup()).thenReturn(mockMetricGroup);
    }

    /**
     * 测试 CUSTOM 模式使用 sql 配置
     * CUSTOM 模式应该使用 config.getSql()，忽略 config.getTable()
     */
    @Test
    public void testCustomModeUsesSqlConfig() throws Exception {
        // Mock Connection 和 PreparedStatement
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doNothing().when(mockConnection).commit();

        // 准备配置：CUSTOM 模式 + sql + table（table 应该被忽略）
        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("password")
            .mode(WriteMode.CUSTOM)
            .sql("INSERT INTO target_table (id, name) VALUES (:id, :name)")
            .table("ignored_table")  // 这个应该被忽略
            .batchSize(100)
            .dialect(new MySQLDialect())
            .build();

        // Mock DriverManager
        MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
        mockedDriverManager.when(() ->
            DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
        ).thenReturn(mockConnection);

        JdbcSinkWriter writer = null;
        try {
            // 创建 Writer
            writer = new JdbcSinkWriter(mockContext, config);

            // 写入一条数据触发 initStatement()
            Row row = Row.withNames();
            row.setField("id", "1");
            row.setField("name", "Alice");
            writer.write(row, null);

            // 验证：应该使用配置的 sql，而不是 table
            // MySQLDialect 不处理 CUSTOM 模式的 SQL，所以直接使用用户提供的 SQL（经过 NamedParameterSqlParser 处理）
            verify(mockConnection).prepareStatement(eq("INSERT INTO target_table (id, name) VALUES (?, ?)"));
            verify(mockStatement, times(2)).setObject(anyInt(), any());  // id 和 name 两个字段
            verify(mockStatement, times(1)).addBatch();

            // 手动 flush，清空 pendingCount
            when(mockStatement.executeBatch()).thenReturn(new int[]{1});
            writer.flush(false);

        } finally {
            // 清理资源
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    // 忽略 close 时的异常
                }
            }
            mockedDriverManager.close();
        }
    }

    /**
     * 测试 INSERT 模式忽略 sql 配置，使用 table
     * INSERT 模式应该使用 config.getTable() 生成 INSERT SQL，忽略 config.getSql()
     */
    @Test
    public void testInsertModeIgnoresSqlConfig() throws Exception {
        // Mock Connection 和 PreparedStatement
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doNothing().when(mockConnection).commit();

        // 准备配置：INSERT 模式 + table + sql（sql 应该被忽略）
        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("password")
            .mode(WriteMode.INSERT)
            .table("test_table")
            .sql("INSERT INTO ignored_table (id) VALUES (:id)")  // 这个应该被忽略
            .batchSize(100)
            .dialect(new MySQLDialect())
            .build();

        // Mock DriverManager
        MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
        mockedDriverManager.when(() ->
            DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
        ).thenReturn(mockConnection);

        JdbcSinkWriter writer = null;
        try {
            // 创建 Writer
            writer = new JdbcSinkWriter(mockContext, config);

            // 写入一条数据触发 initStatement()
            Row row = Row.withNames();
            row.setField("id", "1");
            row.setField("name", "Alice");
            writer.write(row, null);

            // 验证：应该使用 table 生成 INSERT SQL，忽略 sql 配置
            // MySQLDialect 使用反引号包裹表名和字段名，字段顺序基于 Row 的字段名顺序
            verify(mockConnection).prepareStatement(eq("INSERT INTO `test_table` (`name`, `id`) VALUES (?, ?)"));
            verify(mockStatement, times(2)).setObject(anyInt(), any());  // id 和 name 两个字段
            verify(mockStatement, times(1)).addBatch();

            // 手动 flush，清空 pendingCount
            when(mockStatement.executeBatch()).thenReturn(new int[]{1});
            writer.flush(false);

        } finally {
            // 清理资源
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    // 忽略 close 时的异常
                }
            }
            mockedDriverManager.close();
        }
    }

    /**
     * 测试 UPSERT 模式忽略 sql 配置，使用 table
     * UPSERT 模式应该使用 config.getTable() 生成 UPSERT SQL，忽略 config.getSql()
     */
    @Test
    public void testUpsertModeIgnoresSqlConfig() throws Exception {
        // Mock Connection 和 PreparedStatement
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doNothing().when(mockConnection).commit();

        // 准备配置：UPSERT 模式 + table + sql + keyFields（sql 应该被忽略）
        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("password")
            .mode(WriteMode.UPSERT)
            .table("test_table")
            .sql("INSERT INTO ignored_table (id) VALUES (:id)")  // 这个应该被忽略
            .keyFields(Arrays.asList("id"))
            .batchSize(100)
            .dialect(new MySQLDialect())
            .build();

        // Mock DriverManager
        MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
        mockedDriverManager.when(() ->
            DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
        ).thenReturn(mockConnection);

        JdbcSinkWriter writer = null;
        try {
            // 创建 Writer
            writer = new JdbcSinkWriter(mockContext, config);

            // 写入一条数据触发 initStatement()
            Row row = Row.withNames();
            row.setField("id", "1");
            row.setField("name", "Alice");
            writer.write(row, null);

            // 验证：应该使用 table 生成 UPSERT SQL，忽略 sql 配置
            // MySQL UPSERT 格式：INSERT INTO ... ON DUPLICATE KEY UPDATE ...
            // MySQLDialect 使用反引号包裹表名和字段名
            verify(mockConnection).prepareStatement(contains("INSERT INTO `test_table`"));
            verify(mockConnection).prepareStatement(contains("ON DUPLICATE KEY UPDATE"));
            verify(mockStatement, atLeast(2)).setObject(anyInt(), any());  // 至少两个字段（id 和 name）
            verify(mockStatement, times(1)).addBatch();

            // 手动 flush，清空 pendingCount
            when(mockStatement.executeBatch()).thenReturn(new int[]{1});
            writer.flush(false);

        } finally {
            // 清理资源
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    // 忽略 close 时的异常
                }
            }
            mockedDriverManager.close();
        }
    }
}