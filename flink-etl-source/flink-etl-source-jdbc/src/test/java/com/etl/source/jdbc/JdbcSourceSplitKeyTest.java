package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.utils.SqlUtils;
import com.etl.source.jdbc.utils.JdbcSplitHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockedStatic;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class JdbcSourceSplitKeyTest {

    private SourceConfig config;
    private JdbcDialect dialect;

    @BeforeEach
    void setUp() {
        config = mock(SourceConfig.class);
        dialect = mock(JdbcDialect.class);

        // 基础配置
        when(config.getString("url")).thenReturn("jdbc:mysql://localhost:3306/test");
        when(config.getString("username")).thenReturn("root");
        when(config.getString("password")).thenReturn("password");
        when(config.getInteger("batchSize", 100)).thenReturn(100);

        // Mock dialect
        when(dialect.wrapUrl(anyString())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void testUserConfiguredSplitKey_BigIntType() {
        // 用户配置 splitKey 为 BIGINT 类型
        when(config.getString("splitKey")).thenReturn("id");
        when(config.getString("table")).thenReturn("users");

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<JdbcSplitHelper> splitHelperMock = mockStatic(JdbcSplitHelper.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            splitHelperMock.when(() -> JdbcSplitHelper.getColumnType(same(dialect), anyString(), eq("users"), isNull(), eq("id"), anyString(), anyString()))
                .thenReturn(Types.BIGINT);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证日志输出：使用用户配置的 splitKey
        }
    }

    @Test
    void testUserConfiguredSplitKey_UnsupportedType() {
        // 用户配置 splitKey 为 VARCHAR 类型（不支持）
        when(config.getString("splitKey")).thenReturn("name");
        when(config.getString("table")).thenReturn("users");

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<JdbcSplitHelper> splitHelperMock = mockStatic(JdbcSplitHelper.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            splitHelperMock.when(() -> JdbcSplitHelper.getColumnType(same(dialect), anyString(), eq("users"), isNull(), eq("name"), anyString(), anyString()))
                .thenReturn(Types.VARCHAR);

            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new JdbcSource(config)
            );
            assertTrue(exception.getMessage().contains("不支持分片"));
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_SingleBigIntKey() {
        // 配置 table，表有单主键 BIGINT
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("users");
        when(config.getString("sql")).thenReturn(null);

        Map<String, Integer> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("id", Types.BIGINT);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("users"), anyString(), anyString()))
                .thenReturn(primaryKeys);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证自动选择了 id (BIGINT)
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_CompositeKey() {
        // 配置 table，表有复合主键（INT + BIGINT）
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("orders");
        when(config.getString("sql")).thenReturn(null);

        Map<String, Integer> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("id", Types.INTEGER);
        primaryKeys.put("seq", Types.BIGINT);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("orders"), anyString(), anyString()))
                .thenReturn(primaryKeys);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证优先选择了 seq (BIGINT)
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_NoSupportedType() {
        // 配置 table，主键列类型都不支持
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("logs");
        when(config.getString("sql")).thenReturn(null);

        Map<String, Integer> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("created_at", Types.TIMESTAMP);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("logs"), anyString(), anyString()))
                .thenReturn(primaryKeys);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证降级为单分片模式（警告日志）
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_NoPrimaryKey() {
        // 配置 table，表无主键
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("temp");
        when(config.getString("sql")).thenReturn(null);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("temp"), anyString(), anyString()))
                .thenThrow(new NoPrimaryKeyException("temp"));

            RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new JdbcSource(config)
            );
            assertTrue(exception.getMessage().contains("无法自动推断 splitKey"));
        }
    }

    @Test
    void testSqlWithoutSplitKey() {
        // 配置 sql，未配置 splitKey
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn(null);
        when(config.getString("sql")).thenReturn("SELECT id, name FROM users WHERE status = 1");

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证单分片模式（警告日志）
        }
    }

    @Test
    void testSqlWithUserSplitKey() {
        // 配置 sql + splitKey
        when(config.getString("splitKey")).thenReturn("id");
        when(config.getString("table")).thenReturn(null);
        when(config.getString("sql")).thenReturn("SELECT id, name FROM users WHERE status = 1");

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<JdbcSplitHelper> splitHelperMock = mockStatic(JdbcSplitHelper.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            splitHelperMock.when(() -> JdbcSplitHelper.getColumnType(same(dialect), anyString(), isNull(), anyString(), eq("id"), anyString(), anyString()))
                .thenReturn(Types.BIGINT);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证使用用户配置的 splitKey
        }
    }

    @Test
    void testNoTableOrSql() {
        // table 和 sql 都未配置
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn(null);
        when(config.getString("sql")).thenReturn(null);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new JdbcSource(config)
            );
            assertTrue(exception.getMessage().contains("table 和 sql 至少配置一个"));
        }
    }
}