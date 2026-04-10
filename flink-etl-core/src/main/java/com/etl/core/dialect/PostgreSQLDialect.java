package com.etl.core.dialect;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL 数据库方言
 */
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
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.length, "?"));

        String keyFieldsStr = keyFields.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        Set<String> keyFieldSet = new HashSet<>(keyFields);
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFieldSet.contains(col))
                .map(col -> quoteIdentifier(col) + "=EXCLUDED." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                quoteIdentifier(table), colList, placeholders, keyFieldsStr, updateClause);
    }

    @Override
    public String getUpdateSql(String table, String[] columns, List<String> keyFields) {
        throw new UnsupportedOperationException("PostgreSQL CDC 模式暂不支持");
    }

    @Override
    public String getDeleteSql(String table, List<String> keyFields) {
        throw new UnsupportedOperationException("PostgreSQL CDC 模式暂不支持");
    }
}