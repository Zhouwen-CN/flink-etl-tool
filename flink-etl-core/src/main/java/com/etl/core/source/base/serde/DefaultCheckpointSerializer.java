package com.etl.core.source.base.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.etl.core.source.base.BaseEnumCheckpoint;
import com.etl.core.source.base.BaseSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 默认的检查点序列化器
 * 使用 Kryo 序列化，性能优于 Java 原生序列化
 *
 * @param <SplitT>      分片类型
 * @param <CheckpointT> 检查点类型
 */
public class DefaultCheckpointSerializer<SplitT extends BaseSourceSplit,
        CheckpointT extends BaseEnumCheckpoint<SplitT>>
        implements SimpleVersionedSerializer<CheckpointT> {

    private static final int VERSION = 2;

    private static final ThreadLocal<Kryo> KRYO_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo;
    });

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(CheckpointT checkpoint) throws IOException {
        Kryo kryo = KRYO_THREAD_LOCAL.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, checkpoint);
            output.flush();
            return baos.toByteArray();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public CheckpointT deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("版本不匹配，期望版本: " + VERSION + "，实际版本: " + version);
        }
        Kryo kryo = KRYO_THREAD_LOCAL.get();
        try (Input input = new Input(serialized)) {
            return (CheckpointT) kryo.readClassAndObject(input);
        }
    }
}
