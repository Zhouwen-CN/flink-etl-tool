package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * H2 数据库方言（主要用于测试）
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class H2Dialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "h2";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":h2:");
    }

    @Override
    public String wrapUrl(String url) {
        // H2 不需要额外的 URL 参数
        return url;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        // H2 使用 MERGE INTO 语法实现 UPSERT
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.length, "?"));

        String keyColumns = keyFields.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        return String.format("MERGE INTO %s (%s) KEY(%s) VALUES (%s)",
                quoteIdentifier(table), colList, keyColumns, placeholders);
    }

    @Override
    public String getUpdateSql(String table, String[] columns, List<String> keyFields) {
        throw new UnsupportedOperationException("H2 CDC 模式暂不支持");
    }

    @Override
    public String getDeleteSql(String table, List<String> keyFields) {
        throw new UnsupportedOperationException("H2 CDC 模式暂不支持");
    }
}
