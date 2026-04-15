package com.etl.source.mock;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.Getter;

/**
 * Mock Source 单分片
 * 固定 ID: "mock-split-0"
 */
@Getter
public class MockSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /** 分片 ID，固定值 */
    private final String splitId = "mock-split-0";

    /** Mock 配置 */
    private final MockSourceConfig mockConfig;

    public MockSplit(MockSourceConfig mockConfig) {
        this.mockConfig = mockConfig;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "MockSplit{" +
                "splitId='" + splitId + '\'' +
                '}';
    }
}