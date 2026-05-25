package com.etl.connector.mock.source;

import com.etl.core.source.AbstractSplitState;
import lombok.Getter;

/**
 * Mock Split 状态
 */
@Getter
public class MockSplitState extends AbstractSplitState<MockSplit> {

    private static final long serialVersionUID = 1L;

    public MockSplitState(MockSplit split) {
        super(split);
    }
}