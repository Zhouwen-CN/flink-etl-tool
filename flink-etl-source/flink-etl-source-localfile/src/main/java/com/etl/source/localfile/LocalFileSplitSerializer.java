package com.etl.source.localfile;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * LocalFileSplit 序列化器
 * 手动序列化以避免 Kryo 序列化问题
 */
public class LocalFileSplitSerializer implements SimpleVersionedSerializer<LocalFileSplit> {

    private static final int VERSION = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LocalFileSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(split.getFilePath());
            dos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public LocalFileSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version > VERSION) {
            throw new IOException("无法读取未来版本的数据，当前版本: " + VERSION + "，数据版本: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            String filePath = dis.readUTF();
            return new LocalFileSplit(filePath);
        }
    }
}
