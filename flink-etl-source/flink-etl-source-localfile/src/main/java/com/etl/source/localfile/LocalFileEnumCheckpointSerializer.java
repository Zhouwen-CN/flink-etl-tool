package com.etl.source.localfile;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * LocalFileEnumCheckpoint 序列化器
 * 手动序列化以避免 Kryo 序列化问题
 */
public class LocalFileEnumCheckpointSerializer implements SimpleVersionedSerializer<LocalFileEnumCheckpoint> {

    private static final int VERSION = 1;
    private static final LocalFileSplitSerializer SPLIT_SERIALIZER = new LocalFileSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LocalFileEnumCheckpoint checkpoint) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            Collection<LocalFileSplit> splits = checkpoint.getPendingSplits();
            if (splits == null) {
                dos.writeInt(0);
            } else {
                dos.writeInt(splits.size());
                for (LocalFileSplit split : splits) {
                    byte[] splitBytes = SPLIT_SERIALIZER.serialize(split);
                    dos.writeInt(splitBytes.length);
                    dos.write(splitBytes);
                }
            }

            dos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public LocalFileEnumCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("版本不匹配，期望版本: " + VERSION + "，实际版本: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {

            int splitCount = dis.readInt();
            List<LocalFileSplit> splits = new ArrayList<>(splitCount);
            for (int i = 0; i < splitCount; i++) {
                int splitBytesLength = dis.readInt();
                byte[] splitBytes = new byte[splitBytesLength];
                dis.readFully(splitBytes);
                splits.add(SPLIT_SERIALIZER.deserialize(SPLIT_SERIALIZER.getVersion(), splitBytes));
            }

            return new LocalFileEnumCheckpoint(splits);
        }
    }
}