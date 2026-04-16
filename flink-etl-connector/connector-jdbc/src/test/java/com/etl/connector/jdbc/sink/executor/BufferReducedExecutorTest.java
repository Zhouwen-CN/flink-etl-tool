package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.dialect.H2Dialect;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BufferReducedExecutor 测试
 */
class BufferReducedExecutorTest {

    private Connection connection;
    private BufferReducedExecutor executor;

    @BeforeEach
    void setUp() throws SQLException {
        // 创建 H2 内存数据库（每次使用唯一名称避免冲突）
        connection = DriverManager.getConnection("jdbc:h2:mem:test" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        connection.setAutoCommit(false);

        // 创建测试表（使用双引号保持小写，匹配 H2Dialect 的 quoteIdentifier）
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE \"users\" (\"id\" INT PRIMARY KEY, \"name\" VARCHAR(50), \"age\" INT)");
        stmt.close();
        connection.commit();

        // 初始化 Executor
        String[] columns = {"id", "name", "age"};
        List<String> keyFields = Arrays.asList("id");
        executor = new BufferReducedExecutor(
                new H2Dialect(),
                "users",
                columns,
                keyFields
        );
        executor.prepareStatements(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (executor != null) {
            executor.closeStatements();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testKeyReduction_sameKeyMultipleOperations_keepsFinalState() throws SQLException {
        // 同主键多次变更
        Row row1 = Row.withNames();
        row1.setField("id", 1);
        row1.setField("name", "张三");
        row1.setField("age", 20);
        row1.setKind(RowKind.INSERT);

        Row row2 = Row.withNames();
        row2.setField("id", 1);
        row2.setField("name", "李四");
        row2.setField("age", 20);
        row2.setKind(RowKind.UPDATE_AFTER);

        Row row3 = Row.withNames();
        row3.setField("id", 1);
        row3.setField("name", "王五");
        row3.setField("age", 25);
        row3.setKind(RowKind.UPDATE_AFTER);

        executor.addToBatch(row1);
        executor.addToBatch(row2);
        executor.addToBatch(row3);

        // 执行批次
        executor.executeBatch();
        connection.commit();

        // 验证数据库只有最终状态
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"users\" WHERE \"id\" = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("王五", rs.getString("name"));
        assertEquals(25, rs.getInt("age"));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    @Test
    void testUpdateBeforeSkipped_cdcMode_updateBeforeIgnored() throws SQLException {
        // UPDATE_BEFORE 应该被跳过
        Row updateBefore = Row.withNames();
        updateBefore.setField("id", 1);
        updateBefore.setField("name", "张三");
        updateBefore.setField("age", 20);
        updateBefore.setKind(RowKind.UPDATE_BEFORE);

        Row updateAfter = Row.withNames();
        updateAfter.setField("id", 1);
        updateAfter.setField("name", "李四");
        updateAfter.setField("age", 21);
        updateAfter.setKind(RowKind.UPDATE_AFTER);

        executor.addToBatch(updateBefore);
        executor.addToBatch(updateAfter);

        executor.executeBatch();
        connection.commit();

        // 验证数据库只有 UPDATE_AFTER 的数据
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"users\" WHERE \"id\" = 1");
        assertTrue(rs.next());
        assertEquals("李四", rs.getString("name"));
        assertEquals(21, rs.getInt("age"));

        rs.close();
        stmt.close();
    }

    @Test
    void testSegmentedExecution_upsertThenDelete_executedInOrder() throws SQLException {
        // Upsert → Delete 混合
        Row insert = Row.withNames();
        insert.setField("id", 1);
        insert.setField("name", "张三");
        insert.setField("age", 20);
        insert.setKind(RowKind.INSERT);

        Row delete = Row.withNames();
        delete.setField("id", 2);
        delete.setField("name", "李四");
        delete.setField("age", 21);
        delete.setKind(RowKind.DELETE);

        executor.addToBatch(insert);
        executor.addToBatch(delete);

        executor.executeBatch();
        connection.commit();

        // 验证 id=1 存在，id=2 不存在
        Statement stmt = connection.createStatement();

        ResultSet rs1 = stmt.executeQuery("SELECT * FROM \"users\" WHERE \"id\" = 1");
        assertTrue(rs1.next());
        assertEquals("张三", rs1.getString("name"));
        rs1.close();

        ResultSet rs2 = stmt.executeQuery("SELECT * FROM \"users\" WHERE \"id\" = 2");
        assertFalse(rs2.next());
        rs2.close();

        stmt.close();
    }

    @Test
    void testUpsertThenDelete_finalStateIsDelete() throws SQLException {
        // 先 Upsert，后 Delete → 最终状态是 Delete
        Row upsert = Row.withNames();
        upsert.setField("id", 1);
        upsert.setField("name", "张三");
        upsert.setField("age", 20);
        upsert.setKind(RowKind.INSERT);

        Row delete = Row.withNames();
        delete.setField("id", 1);
        delete.setField("name", "张三");
        delete.setField("age", 20);
        delete.setKind(RowKind.DELETE);

        executor.addToBatch(upsert);
        executor.addToBatch(delete);

        executor.executeBatch();
        connection.commit();

        // 验证数据库中没有 id=1
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"users\" WHERE \"id\" = 1");
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }
}
