package com.etl.core.source.serde;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.util.SerializerUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * 默认的分片序列化器
 * 使用 JDK 序列化
 *
 * @param <SplitT> 分片类型
 */
public class DefaultSplitSerializer<SplitT extends BaseSourceSplit> implements SimpleVersionedSerializer<SplitT> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SplitT split) throws IOException {
        return SerializerUtil.serialize(split);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SplitT deserialize(int version, byte[] serialized) throws IOException {
        if (version > VERSION) {
            throw new IOException("无法读取未来版本的数据，当前版本: " + VERSION + "，数据版本: " + version);
        }

        return (SplitT) SerializerUtil.deserialize(serialized);
    }
}
