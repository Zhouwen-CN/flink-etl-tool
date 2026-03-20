package com.etl.core.source.serde;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.BaseSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * 默认的检查点序列化器
 * 使用 JDK 序列化
 *
 * @param <SplitT>      分片类型
 * @param <CheckpointT> 检查点类型
 */
public class DefaultCheckpointSerializer<SplitT extends BaseSourceSplit, CheckpointT extends BaseEnumCheckpoint<SplitT>>
        implements SimpleVersionedSerializer<CheckpointT> {

    public static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(CheckpointT checkpoint) throws IOException {
        return SerializerUtils.serialize(checkpoint);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CheckpointT deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("无法读取未来版本的数据，当前版本: " + VERSION + "，数据版本: " + version);
        }
        return (CheckpointT) SerializerUtils.deserialize(serialized);
    }
}
