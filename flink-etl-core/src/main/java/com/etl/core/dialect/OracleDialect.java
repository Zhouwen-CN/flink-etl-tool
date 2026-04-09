package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Oracle/OceanBase 数据库方言
 * 支持 Oracle 和 OceanBase Oracle 模式
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class OracleDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "oracle";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":oracle:");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        // 参数校验
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table 不能为空");
        }
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException("columns 不能为空");
        }
        if (keyFields == null || keyFields.isEmpty()) {
            throw new IllegalArgumentException("keyFields 不能为空");
        }

        // Oracle 使用 MERGE INTO 语法
        String targetTable = quoteIdentifier(table);

        // 构建列列表
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        // 构建 VALUES 占位符
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.length, "?"));

        // 构建源数据查询（DUAL 表模拟）
        String sourceColumns = Arrays.stream(columns)
                .map(col -> "?" + " AS " + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        // 构建 ON 条件（主键匹配）
        Set<String> keyFieldSet = new HashSet<>(keyFields);
        String onClause = keyFields.stream()
                .map(key -> targetTable + "." + quoteIdentifier(key) + " = source." + quoteIdentifier(key))
                .collect(Collectors.joining(" AND "));

        // 构建 UPDATE 子句（非主键字段）
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFieldSet.contains(col))
                .map(col -> quoteIdentifier(col) + " = source." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        // 构建 INSERT 子句
        String insertValues = Arrays.stream(columns)
                .map(col -> "source." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        return String.format(
                "MERGE INTO %s USING (SELECT %s FROM DUAL) source ON (%s) " +
                "WHEN MATCHED THEN UPDATE SET %s " +
                "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                targetTable, sourceColumns, onClause,
                updateClause, colList, insertValues
        );
    }
}