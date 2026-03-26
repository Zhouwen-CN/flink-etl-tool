package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PostgreSQL 数据库方言
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class PostgreSQLDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "postgresql";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":postgresql:");
    }

    @Override
    public String wrapUrl(String url) {
        // PostgreSQL 默认参数，可根据需要添加
        return url;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getInsertSql(String table, String[] columns) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                quoteIdentifier(table), colList, placeholders);
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        // PostgreSQL 使用 ON CONFLICT ... DO UPDATE
        String keyFieldsStr = keyFields.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        // 更新所有非主键字段
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFields.contains(col))
                .map(col -> quoteIdentifier(col) + "=EXCLUDED." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                quoteIdentifier(table), colList, placeholders, keyFieldsStr, updateClause);
    }
}