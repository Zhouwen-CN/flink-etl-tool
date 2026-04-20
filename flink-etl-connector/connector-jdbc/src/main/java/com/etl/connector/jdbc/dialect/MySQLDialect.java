package com.etl.connector.jdbc.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 数据库方言
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class MySQLDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "mysql";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":mysql:");
    }

    @Override
    public String wrapUrl(String url) {
        if (url == null) {
            return null;
        }

        boolean hasQueryParams = url.contains("?");

        if (!url.contains("useCursorFetch=true")) {
            url = hasQueryParams ? url + "&useCursorFetch=true" : url + "?useCursorFetch=true";
            hasQueryParams = true;
            log.info("MySQL URL 添加 useCursorFetch 参数");
        }
        if (!url.contains("rewriteBatchedStatements=true")) {
            url = hasQueryParams ? url + "&rewriteBatchedStatements=true" : url + "?rewriteBatchedStatements=true";
            log.info("MySQL URL 添加 rewriteBatchedStatements 参数");
        }
        return url;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.length, "?"));

        Set<String> keyFieldSet = new HashSet<>(keyFields);
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFieldSet.contains(col))
                .map(col -> quoteIdentifier(col) + "=VALUES(" + quoteIdentifier(col) + ")")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                quoteIdentifier(table), colList, placeholders, updateClause);
    }

    @Override
    public String hashModExpression(String columnName, int modulus) {
        // MySQL 使用 MD5 函数 + CAST 转为数值
        return String.format("CAST(MD5(%s) AS UNSIGNED) %% %d", columnName, modulus);
    }

    @Override
    public String buildDateRangeQuery(String baseQuery, String columnName,
                                       String startDate, String endDate) {
        if (startDate == null && endDate == null) {
            return baseQuery; // 全表扫描（无分片条件）
        } else if (startDate == null) {
            // 第一个分片：小于结束日期
            return String.format("%s WHERE %s < '%s'", baseQuery, columnName, endDate);
        } else if (endDate == null) {
            // 最后一个分片：大于等于起始日期
            return String.format("%s WHERE %s >= '%s'", baseQuery, columnName, startDate);
        } else {
            // 中间分片：开区间
            return String.format("%s WHERE %s >= '%s' AND %s < '%s'",
                baseQuery, columnName, startDate, columnName, endDate);
        }
    }
}