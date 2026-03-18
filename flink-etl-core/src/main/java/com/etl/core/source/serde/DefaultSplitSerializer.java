package com.etl.core.source.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.etl.core.source.BaseSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 默认的分片序列化器
 * 使用 Kryo 序列化，性能优于 Java 原生序列化
 *
 * @param <SplitT> 分片类型
 */
public class DefaultSplitSerializer<SplitT extends BaseSourceSplit> implements SimpleVersionedSerializer<SplitT> {

    private static final int VERSION = 2;

    // Kryo 非线程安全，使用 ThreadLocal 保证每个线程独立实例
    // StdInstantiatorStrategy 允许反序列化没有无参构造函数的类
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
    public byte[] serialize(SplitT split) throws IOException {
        Kryo kryo = KRYO_THREAD_LOCAL.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, split);
            output.flush();
            return baos.toByteArray();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public SplitT deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("版本不匹配，期望版本: " + VERSION + "，实际版本: " + version);
        }
        Kryo kryo = KRYO_THREAD_LOCAL.get();
        try (Input input = new Input(serialized)) {
            return (SplitT) kryo.readClassAndObject(input);
        }
    }
}
