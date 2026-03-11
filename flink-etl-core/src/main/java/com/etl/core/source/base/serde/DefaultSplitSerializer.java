package com.etl.core.source.base.serde;

import com.etl.core.source.base.BaseSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 默认的分片序列化器
 * 使用 Java 原生序列化，适用于实现了 Serializable 接口的分片
 *
 * <p>注意：对于高性能场景，建议实现自定义序列化器
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
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(split);
            oos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public SplitT deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("版本不匹配，期望版本: " + VERSION + "，实际版本: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (SplitT) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("反序列化失败，类未找到", e);
        }
    }
}