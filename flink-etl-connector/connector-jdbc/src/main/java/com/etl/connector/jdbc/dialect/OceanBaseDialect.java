package com.etl.connector.jdbc.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

/**
 * OceanBase 数据库方言
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class OceanBaseDialect extends OracleDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String driverClassName() {
        return "com.oceanbase.jdbc.Driver";
    }

    @Override
    public String getName() {
        return "oceanbase";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":oceanbase:");
    }
}