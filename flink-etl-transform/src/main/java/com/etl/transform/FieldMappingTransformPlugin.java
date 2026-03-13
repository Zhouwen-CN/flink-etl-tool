package com.etl.transform;

import com.etl.core.config.TransformConfig;
import com.etl.core.spi.TransformPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 字段映射转换插件
 * 支持字段重命名和字段过滤
 */
@Slf4j
public class FieldMappingTransformPlugin implements TransformPlugin {

    @Override
    public String getType() {
        return "field-mapping";
    }

    @Override
    public MapFunction<?, ?> createTransform(TransformConfig config) {
        List<Map<String, String>> mappings = (List<Map<String, String>>) config.get("mappings");
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("field-mapping 配置缺少 mappings");
        }

        List<String> fromFields = new ArrayList<>(mappings.size());
        List<String> toFields = new ArrayList<>(mappings.size());
        for (Map<String, String> mapping : mappings) {
            fromFields.add(mapping.get("from"));
            toFields.add(mapping.get("to"));
        }

        log.info("创建字段映射转换插件, mappings={}", mappings);
        return new FieldMappingFunction(fromFields, toFields);
    }

    /**
     * 字段映射函数
     * 将输入 Row 中配置了映射的字段重命名，未配置的字段原样保留
     */
    private static class FieldMappingFunction implements MapFunction<Row, Row> {
        private static final long serialVersionUID = 1L;

        private final List<String> fromFields;
        private final List<String> toFields;

        public FieldMappingFunction(List<String> fromFields, List<String> toFields) {
            this.fromFields = fromFields;
            this.toFields = toFields;
        }

        @Override
        public Row map(Row input) throws Exception {
            Row output = Row.withNames();
            for (String fieldName : input.getFieldNames(true)) {
                int mappingIndex = fromFields.indexOf(fieldName);
                if (mappingIndex >= 0) {
                    // 该字段有映射，使用新字段名写入
                    output.setField(toFields.get(mappingIndex), input.getField(fieldName));
                } else {
                    // 无映射，原样保留
                    output.setField(fieldName, input.getField(fieldName));
                }
            }
            return output;
        }
    }
}