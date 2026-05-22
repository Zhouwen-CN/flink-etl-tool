package com.etl.connector.mock.source;

import com.etl.core.source.AbstractSplitState;
import lombok.Getter;

/**
 * Mock Split 状态
 */
@Getter
public class MockSplitState extends AbstractSplitState<MockSplit> {

    public MockSplitState(MockSplit split) {
        super(split);
    }
}