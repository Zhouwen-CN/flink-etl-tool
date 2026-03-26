package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
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
        // MySQL 需要添加 useCursorFetch 参数，使 batchSize 生效
        if (url != null && !url.contains("useCursorFetch=true")) {
            url = url.contains("?") ? url + "&useCursorFetch=true" : url + "?useCursorFetch=true";
            log.info("MySQL URL 添加 useCursorFetch 参数");
        }
        // 添加 rewriteBatchedStatements 参数，优化批量写入
        if (url != null && !url.contains("rewriteBatchedStatements=true")) {
            url = url.contains("?") ? url + "&rewriteBatchedStatements=true" : url + "?rewriteBatchedStatements=true";
            log.info("MySQL URL 添加 rewriteBatchedStatements 参数");
        }
        return url;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
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

        // MySQL 使用 ON DUPLICATE KEY UPDATE
        // 更新所有非主键字段
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFields.contains(col))
                .map(col -> quoteIdentifier(col) + "=VALUES(" + quoteIdentifier(col) + ")")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                quoteIdentifier(table), colList, placeholders, updateClause);
    }
}