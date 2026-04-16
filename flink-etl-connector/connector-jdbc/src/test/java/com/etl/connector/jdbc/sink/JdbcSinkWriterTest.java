package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.dialect.H2Dialect;
import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.dialect.WriteMode;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
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
        // 使用唯一数据库名避免测试间冲突
        // H2 2.x 改变了默认认证：需要通过 INIT 参数显式创建用户
        String dbName = "upsert_test_" + System.nanoTime();
        // INIT=... 在首次连接时执行，提前创建 sa 用户
        String jdbcUrl = "jdbc:h2:mem:" + dbName + ";MODE=MySQL;INIT=CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN";

        // 测试代码通过 JdbcSinkWriter 使用的同一凭证连接（sa / 空密码）
        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url(jdbcUrl)
            .username("sa")
            .password("")
            .mode(WriteMode.UPSERT)
            .table("test_table")
            .sql("INSERT INTO ignored_table (id) VALUES (:id)")
            .keyFields(Arrays.asList("id"))
            .batchSize(100)
            .dialect(new H2Dialect())
            .build();

        JdbcSinkWriter writer = new JdbcSinkWriter(mockContext, config);

        // JdbcSinkWriter 已创建了数据库连接，再建一个用于验证
        Connection realConnection = DriverManager.getConnection(jdbcUrl, "sa", "");
        realConnection.setAutoCommit(false);

        Statement stmt = realConnection.createStatement();
        stmt.execute("CREATE TABLE \"test_table\" (\"id\" INT PRIMARY KEY, \"name\" VARCHAR(50))");
        stmt.close();
        realConnection.commit();

        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "Alice");
        writer.write(row, null);
        writer.flush(false);
        realConnection.commit();

        Statement queryStmt = realConnection.createStatement();
        ResultSet rs = queryStmt.executeQuery("SELECT * FROM \"test_table\" WHERE \"id\" = 1");
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("name"));
        rs.close();
        queryStmt.close();

        writer.close();
        realConnection.close();
    }

    /**
     * 测试 INSERT 模式过滤 __ 开头的隐藏字段
     * 当 Row 包含 __topic__ 等隐藏字段时，这些字段不应被写入 SQL
     */
    @Test
    public void testInsertModeIgnoresHiddenFields() throws Exception {
        // Mock Connection 和 PreparedStatement
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doNothing().when(mockConnection).commit();

        // 准备配置：INSERT 模式
        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("password")
            .mode(WriteMode.INSERT)
            .table("test_table")
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
            writer = new JdbcSinkWriter(mockContext, config);

            // 写入包含隐藏字段的 Row（模拟 Kafka Source 输出的数据）
            Row row = Row.withNames();
            row.setField("id", "1");
            row.setField("name", "Alice");
            row.setField("__topic__", "test-topic");  // 隐藏字段，应该被忽略
            row.setField("__partition__", 0);          // 隐藏字段，应该被忽略
            writer.write(row, null);

            // 验证：SQL 中不应包含 __topic__ 和 __partition__ 字段
            verify(mockConnection).prepareStatement(
                eq("INSERT INTO `test_table` (`name`, `id`) VALUES (?, ?)")
            );
            // 只验证 id 和 name 两个字段，不包含隐藏字段
            verify(mockStatement, times(2)).setObject(anyInt(), any());

            // 手动 flush
            when(mockStatement.executeBatch()).thenReturn(new int[]{1});
            writer.flush(false);

        } finally {
            if (writer != null) {
                try { writer.close(); } catch (Exception e) {}
            }
            mockedDriverManager.close();
        }
    }

    /**
     * 测试 CDC INSERT 模式使用 upsert SQL
     * 验证 CDC INSERT 模式生成 UPSERT SQL（原子操作）
     */
    @Test
    public void testCdcInsertModeUsesUpsertSql() throws Exception {
        // H2 2.x INIT 参数创建 sa 用户（空密码）
        String dbName = "cdc_insert_test_" + System.nanoTime();
        String jdbcUrl = "jdbc:h2:mem:" + dbName + ";MODE=MySQL;INIT=CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN";

        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url(jdbcUrl)
            .username("sa")
            .password("")
            .mode(WriteMode.CDC)
            .table("test_table")
            .keyFields(Arrays.asList("id"))
            .batchSize(100)
            .dialect(new H2Dialect())
            .build();

        JdbcSinkWriter writer = new JdbcSinkWriter(mockContext, config);

        // JdbcSinkWriter 已创建连接，用相同凭证再创建一个用于建表和验证
        Connection realConnection = DriverManager.getConnection(jdbcUrl, "sa", "");
        realConnection.setAutoCommit(false);

        Statement stmt = realConnection.createStatement();
        stmt.execute("CREATE TABLE \"test_table\" (\"id\" INT PRIMARY KEY, \"name\" VARCHAR(50))");
        stmt.close();
        realConnection.commit();

        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "Alice");
        row.setKind(RowKind.INSERT);
        writer.write(row, null);

        writer.flush(false);
        realConnection.commit();

        Statement queryStmt = realConnection.createStatement();
        ResultSet rs = queryStmt.executeQuery("SELECT * FROM \"test_table\" WHERE \"id\" = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
        assertFalse(rs.next());
        rs.close();
        queryStmt.close();

        writer.close();
        realConnection.close();
    }

    /**
     * 测试 CDC UPDATE_AFTER 模式使用 upsert SQL
     * 验证 CDC UPDATE_AFTER 模式生成 UPSERT SQL（原子操作）
     */
    @Test
    public void testCdcUpdateAfterModeUsesUpsertSql() throws Exception {
        // H2 2.x INIT 参数创建 sa 用户（空密码）
        String dbName = "cdc_update_test_" + System.nanoTime();
        String jdbcUrl = "jdbc:h2:mem:" + dbName + ";MODE=MySQL;INIT=CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN";

        // 先由 JdbcSinkWriter 创建连接（会执行 INIT）
        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url(jdbcUrl)
            .username("sa")
            .password("")
            .mode(WriteMode.CDC)
            .table("test_table")
            .keyFields(Arrays.asList("id"))
            .batchSize(100)
            .dialect(new H2Dialect())
            .build();

        JdbcSinkWriter writer = new JdbcSinkWriter(mockContext, config);

        // JdbcSinkWriter 已初始化连接，再用相同凭证创建一个用于建表和验证
        Connection realConnection = DriverManager.getConnection(jdbcUrl, "sa", "");
        realConnection.setAutoCommit(false);

        Statement stmt = realConnection.createStatement();
        stmt.execute("CREATE TABLE \"test_table\" (\"id\" INT PRIMARY KEY, \"name\" VARCHAR(50))");
        stmt.close();
        realConnection.commit();

        // 先用 MERGE 插入一条初始数据
        Statement insertStmt = realConnection.createStatement();
        insertStmt.execute("MERGE INTO \"test_table\" (\"id\", \"name\") KEY(\"id\") VALUES (1, 'Bob')");
        insertStmt.close();
        realConnection.commit();

        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "Alice-updated");
        row.setKind(RowKind.UPDATE_AFTER);
        writer.write(row, null);

        writer.flush(false);
        realConnection.commit();

        Statement queryStmt = realConnection.createStatement();
        ResultSet rs = queryStmt.executeQuery("SELECT * FROM \"test_table\" WHERE \"id\" = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice-updated", rs.getString("name"));
        assertFalse(rs.next());
        rs.close();
        queryStmt.close();

        writer.close();
        realConnection.close();
    }

    /**
     * 测试 CDC DELETE 模式当前行为
     * 验证 CDC DELETE 模式生成 DELETE SQL
     */
    @Test
    public void testCdcDeleteModeBehavior() throws Exception {
        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doNothing().when(mockConnection).commit();

        JdbcSinkConfig config = JdbcSinkConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("password")
            .mode(WriteMode.CDC)
            .table("test_table")
            .keyFields(Arrays.asList("id"))
            .batchSize(100)
            .dialect(new MySQLDialect())
            .build();

        MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
        mockedDriverManager.when(() ->
            DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
        ).thenReturn(mockConnection);

        JdbcSinkWriter writer = null;
        try {
            writer = new JdbcSinkWriter(mockContext, config);

            Row row = Row.withNames();
            row.setField("id", "1");
            row.setField("name", "Alice");
            row.setKind(RowKind.DELETE);
            writer.write(row, null);

            // 验证 DELETE SQL
            verify(mockConnection).prepareStatement(eq("DELETE FROM `test_table` WHERE `id` = ?"));

            when(mockStatement.executeBatch()).thenReturn(new int[]{1});
            writer.flush(false);
        } finally {
            if (writer != null) {
                try { writer.close(); } catch (Exception e) {}
            }
            mockedDriverManager.close();
        }
    }
}