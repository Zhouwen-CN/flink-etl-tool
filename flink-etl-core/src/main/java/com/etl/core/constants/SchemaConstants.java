package com.etl.core.constants;

import java.time.format.DateTimeFormatter;

/**
 * Schema 相关的公共常量
 */
public final class SchemaConstants {

    private SchemaConstants() {
    }

    /**
     * 默认时间戳格式（用于 TypeConverter、JsonToRowConverter、RowToJsonConverter）
     */
    public static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
}
