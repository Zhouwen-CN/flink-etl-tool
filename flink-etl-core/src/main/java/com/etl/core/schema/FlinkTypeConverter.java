package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Flink 类型转换器
 * 将 EtlSchema 转换为 Flink RowType
 */
public class FlinkTypeConverter {

    /**
     * 将 EtlFieldType 转换为 Flink DataTypes
     */
    public static TypeInformation<?> fromEtlType(EtlFieldType type) {
        switch (type) {
            case STRING:
                return org.apache.flink.api.common.typeinfo.Types.STRING;
            case BOOLEAN:
                return org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
            case INT:
                return org.apache.flink.api.common.typeinfo.Types.INT;
            case LONG:
                return org.apache.flink.api.common.typeinfo.Types.LONG;
            case DOUBLE:
                return org.apache.flink.api.common.typeinfo.Types.DOUBLE;
            case DECIMAL:
                return org.apache.flink.api.common.typeinfo.Types.BIG_DEC;
            case TIMESTAMP:
                return org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME;
            default:
                throw new IllegalArgumentException("不支持的类型：" + type);
        }
    }

    /**
     * 根据 JDBC java.sql.Types 转换为 Flink TypeInformation
     *
     * @param sqlType JDBC SQL 类型常量（来自 java.sql.Types）
     * @return 对应的 Flink TypeInformation
     */
    public static TypeInformation<?> fromSqlType(int sqlType) {
        switch (sqlType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NCLOB:
                return org.apache.flink.api.common.typeinfo.Types.STRING;

            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return org.apache.flink.api.common.typeinfo.Types.BOOLEAN;

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                return org.apache.flink.api.common.typeinfo.Types.INT;

            case java.sql.Types.BIGINT:
                return org.apache.flink.api.common.typeinfo.Types.LONG;

            case java.sql.Types.REAL:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
                return org.apache.flink.api.common.typeinfo.Types.DOUBLE;

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return org.apache.flink.api.common.typeinfo.Types.BIG_DEC;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME;


            /*
            // 待扩展
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                return org.apache.flink.api.common.typeinfo.Types.STRING;
            */

            default:
                // 对于未知类型，默认返回 STRING
                return org.apache.flink.api.common.typeinfo.Types.STRING;
        }
    }
}
