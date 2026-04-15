package com.etl.connector.mock.source;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;

/**
 * Mock Split 状态
 */
@Getter
public class MockSplitState extends BaseSplitState<MockSplit> {

    public MockSplitState(MockSplit split) {
        super(split);
    }
}