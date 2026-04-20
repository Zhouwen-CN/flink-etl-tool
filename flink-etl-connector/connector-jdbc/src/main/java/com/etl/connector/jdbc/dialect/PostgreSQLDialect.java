package com.etl.connector.jdbc.dialect;

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
    public String hashModExpression(String columnName, int modulus) {
        // PostgreSQL 有内置的 hashtext 函数
        return String.format("ABS(hashtext(%s) %% %d)", columnName, modulus);
    }

    @Override
    public String buildDateRangeQuery(String baseQuery, String columnName,
                                       String startDate, String endDate) {
        if (startDate == null && endDate == null) {
            return baseQuery;
        } else if (startDate == null) {
            return String.format("%s WHERE %s < '%s'", baseQuery, columnName, endDate);
        } else if (endDate == null) {
            return String.format("%s WHERE %s >= '%s'", baseQuery, columnName, startDate);
        } else {
            return String.format("%s WHERE %s >= '%s' AND %s < '%s'",
                baseQuery, columnName, startDate, columnName, endDate);
        }
    }
}