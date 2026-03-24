package com.etl.core.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * 命令行参数解析器
 * <p>
 * 负责解析命令行参数并加载对应的 Job 配置
 */
@Slf4j
public class CliArgumentParser {

    /**
     * Base64 编码字符串的正则表达式
     * Base64 字符集：A-Z, a-z, 0-9, +, /，末尾可能有 = 填充
     */
    private static final Pattern BASE64_PATTERN = Pattern.compile(
            "^[A-Za-z0-9+/]+={0,2}$"
    );

    private CliArgumentParser() {
        // 私有构造函数，防止实例化
    }

    /**
     * 解析命令行参数并返回 Job 配置
     *
     * @param args 命令行参数
     * @return Job 配置，如果参数无效则返回 null
     */
    public static JobConfig parse(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has("file")) {
            return loadFromFile(params.get("file"));
        } else if (params.has("config")) {
            return loadFromJsonString(params.get("config"));
        } else {
            printUsage();
            throw new RuntimeException("Missing required argument");
        }
    }

    /**
     * 打印使用说明
     */
    public static void printUsage() {
        System.err.println("用法:");
        System.err.println("  java -jar flink-etl-tool.jar --file <config.json>");
        System.err.println("  java -jar flink-etl-tool.jar --config '<json-string>'");
        System.err.println("  java -jar flink-etl-tool.jar --config '<base64-encoded-json>'");
        System.err.println();
        System.err.println("参数:");
        System.err.println("  --file <path>      从文件加载配置");
        System.err.println("  --config <json>    从 JSON 字符串加载配置（支持 Base64 编码）");
        System.err.println();
        System.err.println("示例:");
        System.err.println("  java -jar flink-etl-tool.jar --file config/mysql-to-console.json");
        System.err.println("  java -jar flink-etl-tool.jar --config '{\"job\":{\"name\":\"test\",\"mode\":\"batch\"},...}'");
        System.err.println("  java -jar flink-etl-tool.jar --config 'eyJqb2IiOnt9fQ=='");
        System.err.println();
    }

    /**
     * 从文件加载配置
     *
     * @param filePath 配置文件路径
     * @return Job 配置对象，参数无效时返回 null
     */
    private static JobConfig loadFromFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            log.error("--file 参数值不能为空");
            return null;
        }

        if (!Files.exists(Paths.get(filePath))) {
            String errorMsg = String.format("配置文件不存在: %s", filePath);
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        if (!new File(filePath).isFile()) {
            String errorMsg = String.format("路径不是文件: %s", filePath);
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        log.info("从文件加载配置: {}", filePath);
        return ConfigParser.parse(filePath);
    }

    /**
     * 从 JSON 字符串加载配置
     * <p>
     * 支持直接传入 JSON 字符串或 Base64 编码的 JSON 字符串。
     * 如果输入是有效的 Base64 编码，会自动解码后解析。
     *
     * @param input JSON 字符串或 Base64 编码的 JSON 字符串
     * @return Job 配置对象，参数无效时返回 null
     */
    private static JobConfig loadFromJsonString(String input) {
        if (input == null || input.trim().isEmpty()) {
            log.error("--config 参数值不能为空");
            return null;
        }

        String json = input;

        // 检测是否为 Base64 编码，如果是则解码
        if (isBase64Encoded(input)) {
            log.info("检测到 Base64 编码的配置，正在解码...");
            json = decodeBase64(input);
            if (json == null) {
                log.error("Base64 解码失败");
                return null;
            }
            log.debug("解码后的配置内容长度: {}", json.length());
        }

        log.info("从命令行 JSON 字符串加载配置");
        return ConfigParser.parseFromString(json);
    }

    /**
     * 判断字符串是否为有效的 Base64 编码
     *
     * @param input 待检测的字符串
     * @return 如果是有效的 Base64 编码返回 true，否则返回 false
     */
    private static boolean isBase64Encoded(String input) {
        String trimmed = input.trim();

        // 空字符串或太短的字符串不可能是有效的 Base64 编码的 JSON
        if (trimmed.length() < 2) {
            return false;
        }

        // Base64 编码的字符串长度必须是 4 的倍数
        if (trimmed.length() % 4 != 0) {
            return false;
        }

        // 检查字符集是否符合 Base64 规范
        if (!BASE64_PATTERN.matcher(trimmed).matches()) {
            return false;
        }

        // 尝试解码，验证是否为有效的 Base64
        try {
            byte[] decoded = Base64.getDecoder().decode(trimmed);
            // 解码后的内容应该是 UTF-8 文本（JSON 字符串）
            String decodedString = new String(decoded, StandardCharsets.UTF_8);

            // 简单校验解码后的内容是否像 JSON（以 { 或 [ 开头）
            String trimmedDecoded = decodedString.trim();
            return trimmedDecoded.startsWith("{") || trimmedDecoded.startsWith("[");
        } catch (IllegalArgumentException e) {
            // 不是有效的 Base64 编码
            return false;
        }
    }

    /**
     * 解码 Base64 字符串
     *
     * @param base64String Base64 编码的字符串
     * @return 解码后的字符串，解码失败返回 null
     */
    private static String decodeBase64(String base64String) {
        try {
            byte[] decoded = Base64.getDecoder().decode(base64String.trim());
            return new String(decoded, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            log.error("Base64 解码失败: {}", e.getMessage());
            return null;
        }
    }
}