package com.etl.connector.localfile.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * 本地文件 Source Reader
 * 继承 AbstractSourceReader，自动处理线程模型和状态管理
 */
public class LocalFileSourceReader extends AbstractSourceReader<Row, Row, LocalFileSplit> {

    public LocalFileSourceReader(
            Supplier<AbstractSplitReader<Row, LocalFileSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }
}
