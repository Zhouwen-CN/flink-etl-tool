package com.etl.source.mock;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * Mock Source 分片占位实现
 * TODO: Task 10 将实现完整的分片逻辑
 */
public class MockSplit implements SourceSplit {

    @Override
    public String splitId() {
        return "mock-split-placeholder";
    }
}