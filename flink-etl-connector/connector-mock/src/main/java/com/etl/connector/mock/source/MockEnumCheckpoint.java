package com.etl.connector.mock.source;

import com.etl.core.source.AbstractEnumCheckpoint;
import lombok.Getter;

import java.util.List;

/**
 * Mock Source Enumerator 检查点
 */
@Getter
public class MockEnumCheckpoint extends AbstractEnumCheckpoint<MockSplit> {
    private static final long serialVersionUID = 1L;

    public MockEnumCheckpoint(List<MockSplit> pendingSplits) {
        super(pendingSplits);
    }
}