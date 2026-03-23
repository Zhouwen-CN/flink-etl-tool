package com.etl.core.utils;

import com.jayway.jsonpath.JsonPath;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * JSON 工具类
 * 封装 Jackson ObjectMapper 的常用操作
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // 忽略未知属性
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private JsonUtils() {
        // 私有构造函数，防止实例化
    }

    /**
     * 对象转 JSON 字符串
     *
     * @param obj 对象
     * @return JSON 字符串
     * @throws IllegalArgumentException 序列化失败时抛出
     */
    public static String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 序列化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 对象转 JsonNode
     *
     * @param obj 对象
     * @return JsonNode
     */
    public static JsonNode valueToTree(Object obj) {
        if (obj == null) {
            return null;
        }
        return MAPPER.valueToTree(obj);
    }

    /**
     * JSON 字符串转对象
     *
     * @param json  JSON 字符串
     * @param clazz 目标类型
     * @param <T>   泛型
     * @return 对象
     * @throws IllegalArgumentException 反序列化失败时抛出
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 反序列化失败: " + e.getMessage(), e);
        }
    }

    /**
     * JSON 文件转对象
     *
     * @param file  JSON 文件
     * @param clazz 目标类型
     * @param <T>   泛型
     * @return 对象
     * @throws IllegalArgumentException 反序列化失败时抛出
     */
    public static <T> T fromFile(File file, Class<T> clazz) {
        try {
            return MAPPER.readValue(file, clazz);
        } catch (IOException e) {
            throw new IllegalArgumentException("JSON 反序列化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 通过 jsonpath 解析 json字符串
     *
     * @param json     JSON 字符串
     * @param jsonPath jsonPath
     * @return jsonNode
     */
    public static JsonNode getByJsonPath(String json, String jsonPath) {
        if (json == null || json.isEmpty()) {
            return null;
        }

        JsonNode jsonNode;
        if (jsonPath != null && !jsonPath.isEmpty()) {
            Object read = JsonPath.read(json, jsonPath);
            jsonNode = valueToTree(read);
        } else {
            jsonNode = valueToTree(json);
        }

        return jsonNode;
    }
}