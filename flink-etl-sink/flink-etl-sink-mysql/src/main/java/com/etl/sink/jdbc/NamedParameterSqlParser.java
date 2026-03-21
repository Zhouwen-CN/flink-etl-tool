package com.etl.sink.jdbc;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 具名占位符 SQL 解析器
 * 将 :paramName 格式转换为 PreparedStatement 的 ? 格式
 */
public class NamedParameterSqlParser {

    private static final Pattern NAMED_PARAM_PATTERN = Pattern.compile(":([a-zA-Z_][a-zA-Z0-9_]*)");

    @Getter
    @RequiredArgsConstructor
    public static class ParsedSql {
        private final String preparedSql;
        private final List<String> paramNames;
    }

    /**
     * 解析具名占位符 SQL
     * @param sql 包含 :paramName 占位符的 SQL
     * @return 解析结果，包含转换后的 SQL 和参数名列表
     */
    public static ParsedSql parse(String sql) {
        List<String> paramNames = new ArrayList<>();
        Matcher matcher = NAMED_PARAM_PATTERN.matcher(sql);
        StringBuilder sb = new StringBuilder();

        while (matcher.find()) {
            paramNames.add(matcher.group(1));
            matcher.appendReplacement(sb, "?");
        }
        matcher.appendTail(sb);

        return new ParsedSql(sb.toString(), paramNames);
    }
}