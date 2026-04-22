package com.etl.connector.cdc.mysql;

import com.etl.core.config.SourceConfig;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MySQL CDC 配置封装类
 */
@Getter
@Builder
public class MySqlCdcConfig implements Serializable {
    private final String url;
    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String table;
    private final StartupMode startupMode;
    private final Long startupTimestamp;
    private final Integer serverId;

    /**
     * 从 SourceConfig 解析配置参数
     */
    public static MySqlCdcConfig fromSourceConfig(SourceConfig config) {

        // 解析 URL
        String url = config.getString("url");
        UrlParseResult urlResult = parseUrl(url);

        // 解析其他参数
        String username = config.getString("username");
        Preconditions.checkNotNull(username, "username 参数不能为空");

        String password = config.getString("password");
        Preconditions.checkNotNull(password, "password 参数不能为空");

        String table = config.getString("table");
        Preconditions.checkNotNull(table, "table 参数不能为空");

        // 解析启动模式
        String startupModeStr = config.getString("startupMode", "latest");
        StartupMode startupMode = StartupMode.of(startupModeStr.toUpperCase());

        // timestamp 模式校验
        Long startupTimestamp = null;
        if (startupMode == StartupMode.TIMESTAMP) {
            startupTimestamp = config.getLong("startupTimestamp");
            Preconditions.checkNotNull(startupTimestamp, "startupMode=timestamp 时必须配置 startupTimestamp");
        }

        // serverId（可选）
        Integer serverId = config.getInteger("serverId", generateAutoServerId());

        return MySqlCdcConfig.builder()
                .url(url)
                .hostname(urlResult.hostname)
                .port(urlResult.port)
                .database(urlResult.database)
                .username(username)
                .password(password)
                .table(table)
                .startupMode(startupMode)
                .startupTimestamp(startupTimestamp)
                .serverId(serverId)
                .build();
    }

    /**
     * URL 正则解析：jdbc:mysql://host:port/database
     */
    private static UrlParseResult parseUrl(String url) {
        // URL 格式校验
        if (url == null || !url.startsWith("jdbc:mysql://")) {
            throw new IllegalArgumentException("URL 必须以 jdbc:mysql:// 开头");
        }

        // 正则解析（支持带参数的 URL）
        Pattern pattern = Pattern.compile("jdbc:mysql://([^:]+):(\\d+)/([^?]+)(?:\\?.*)?");
        Matcher matcher = pattern.matcher(url);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("URL 格式错误，应为 jdbc:mysql://host:port/database");
        }

        String hostname = matcher.group(1);
        int port = Integer.parseInt(matcher.group(2));
        String database = matcher.group(3);

        return new UrlParseResult(hostname, port, database);
    }

    /**
     * 自动生成 serverId（范围：5400-15400）
     * 用于避免多任务冲突
     */
    private static int generateAutoServerId() {
        return (int) (System.currentTimeMillis() % 10000) + 5400;
    }

    /**
     * 获取 StartupOptions（用于 MySqlSource.builder）
     */
    public StartupOptions getStartupOptions() {
        switch (startupMode) {
            case EARLIEST:
                return StartupOptions.earliest();
            case LATEST:
                return StartupOptions.latest();
            case TIMESTAMP:
                return StartupOptions.timestamp(startupTimestamp);
            case SNAPSHOT_FIRST:
                return StartupOptions.initial();
            default:
                throw new IllegalArgumentException("不支持的启动模式: " + startupMode);
        }
    }

    /**
     * URL 解析结果内部类
     */
    private static class UrlParseResult {
        final String hostname;
        final int port;
        final String database;

        UrlParseResult(String hostname, int port, String database) {
            this.hostname = hostname;
            this.port = port;
            this.database = database;
        }
    }
}