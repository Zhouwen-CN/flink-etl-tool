package com.etl.core.schema.metadata;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.types.Row;

/**
 * metadata 字段有严格顺序，和 type 保持一致
 */
@Getter
@Builder
@ToString
public class Metadata {
    private String topic;
    private String source;

    public Row toRow() {
        return Row.of(topic, source);
    }
}