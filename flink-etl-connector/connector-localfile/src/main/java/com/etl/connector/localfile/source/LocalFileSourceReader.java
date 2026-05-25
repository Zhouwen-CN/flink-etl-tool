package com.etl.connector.localfile.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * 本地文件 Source Reader
 * 继承 AbstractSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class LocalFileSourceReader extends AbstractSourceReader<Row, Row, LocalFileSplit, LocalFileSplitState> {

    public LocalFileSourceReader(
            Supplier<AbstractSplitReader<Row, LocalFileSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }

    @Override
    public LocalFileSplitState initializedState(LocalFileSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new LocalFileSplitState(split);
    }
}