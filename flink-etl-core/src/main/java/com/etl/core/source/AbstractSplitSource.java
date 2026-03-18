package com.etl.core.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlField;
import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.FlinkTypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <SplitT> 分片类型
 * @param <CheckpointT> 检查点类型
 */
public abstract class AbstractSplitSource<SplitT extends SourceSplit, CheckpointT>
        implements Source<Row, SplitT, CheckpointT>, ResultTypeQueryable<Row> {

    private final SourceConfig config;

    public AbstractSplitSource(SourceConfig config) {
        this.config = config;
    }

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT> restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext, CheckpointT checkpoint);

    @Override
    public abstract SourceReader<Row, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer();

    /**
     * 默认从 source.schema 中获取，子类可以重写
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            return null;
        }

        // 使用 schema 字段名重建 Row，确保 Flink Table API 能识别列名
        List<String> fieldNames = schema.getFieldNames();
        List<EtlField> fields = schema.getFields();

        // 构建 RowTypeInfo 用于 Flink Table API
        TypeInformation<?>[] typeInfos = fields.stream()
                .map(f -> FlinkTypeConverter.fromEtlType(f.getType()))
                .toArray(TypeInformation<?>[]::new);
        String[] names = fieldNames.toArray(new String[0]);
        return Types.ROW_NAMED(names, typeInfos);
    }
}