package com.etl.source.mock;

import com.etl.core.source.BaseSourceSplit;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.Getter;

/**
 * Mock Source 单分片
 * 固定 ID: "mock-split-0"
 */
@Getter
public class MockSplit extends BaseSourceSplit {

    private final MockSourceConfig mockConfig;

    public MockSplit(MockSourceConfig mockConfig) {
        super("mock-split-0");  // 固定分片 ID
        this.mockConfig = mockConfig;
    }
}