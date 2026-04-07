package com.etl.transform;

import com.etl.core.config.TransformConfig;
import com.etl.core.spi.TransformPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * SQL Transform 插件
 * 支持通过 SQL 语句进行数据转换
 */
@Slf4j
@AutoService(TransformPlugin.class)
public class SqlTransformPlugin implements TransformPlugin {

    @Override
    public String identifier() {
        return "sql";
    }

    @Override
    public Table transform(TransformConfig config, StreamTableEnvironment stEnv) {
        String sql = config.getString("sql");

        // 参数校验
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL Transform 配置缺少 'sql' 字段");
        }

        log.info("执行 SQL: {}", sql);

        try {
            return stEnv.sqlQuery(sql);
        } catch (Exception e) {
            throw new RuntimeException("SQL 执行失败: " + e.getMessage(), e);
        }
    }
}