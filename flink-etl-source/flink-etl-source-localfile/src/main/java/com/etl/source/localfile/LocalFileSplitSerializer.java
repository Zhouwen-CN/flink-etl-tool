package com.etl.source.localfile;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * LocalFileSplit 序列化器
 * 手动序列化以避免 Kryo 序列化问题
 */
public class LocalFileSplitSerializer implements SimpleVersionedSerializer<LocalFileSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LocalFileSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // 写入文件路径
            dos.writeUTF(split.getFilePath());

            // 写入字段数量
            List<String> fields = split.getFields();
            if (fields == null) {
                dos.writeInt(0);
            } else {
                dos.writeInt(fields.size());
                for (String field : fields) {
                    dos.writeUTF(field);
                }
            }

            dos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public LocalFileSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("版本不匹配，期望版本: " + VERSION + "，实际版本: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {

            // 读取文件路径
            String filePath = dis.readUTF();

            // 读取字段列表
            int fieldCount = dis.readInt();
            List<String> fields = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                fields.add(dis.readUTF());
            }

            return new LocalFileSplit(filePath, fields);
        }
    }
}