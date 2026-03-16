package com.etl.source.localfile;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LocalFileSplitSerializer 测试
 */
class LocalFileSplitSerializerTest {

    private final LocalFileSplitSerializer serializer = new LocalFileSplitSerializer();

    @Test
    void testSerializeAndDeserialize() throws IOException {
        // 创建测试分片
        List<String> fields = Arrays.asList("id", "name", "age");
        LocalFileSplit original = new LocalFileSplit("/data/test.csv", fields);

        // 序列化
        byte[] serialized = serializer.serialize(original);

        // 反序列化
        LocalFileSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        // 验证
        assertEquals(original.getSplitId(), deserialized.getSplitId());
        assertEquals(original.getFilePath(), deserialized.getFilePath());
        assertEquals(original.getFileName(), deserialized.getFileName());
        assertEquals(original.getFields(), deserialized.getFields());
    }

    @Test
    void testSerializeWithEmptyFields() throws IOException {
        // 创建没有字段的分片
        LocalFileSplit original = new LocalFileSplit("/data/test.csv", List.of());

        // 序列化
        byte[] serialized = serializer.serialize(original);

        // 反序列化
        LocalFileSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        // 验证
        assertEquals(original.getSplitId(), deserialized.getSplitId());
        assertTrue(deserialized.getFields().isEmpty());
    }

    @Test
    void testVersionMismatch() throws IOException {
        LocalFileSplit split = new LocalFileSplit("/data/test.csv", Arrays.asList("a", "b"));
        byte[] serialized = serializer.serialize(split);

        // 使用错误版本号反序列化应该抛出异常
        assertThrows(IOException.class, () -> {
            serializer.deserialize(999, serialized);
        });
    }
}