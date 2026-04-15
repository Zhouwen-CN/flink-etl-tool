package com.etl.source.mock;

import com.etl.core.source.BaseEnumCheckpoint;
import lombok.Getter;

import java.util.List;

/**
 * Mock Source Enumerator 检查点
 */
@Getter
public class MockEnumCheckpoint extends BaseEnumCheckpoint<MockSplit> {

    public MockEnumCheckpoint(List<MockSplit> pendingSplits) {
        super(pendingSplits);
    }
}