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
            throw new IllegalArgumentException("缺少必要参数：请指定 --file 或 --config");
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

        // 尝试解码 Base64（如果不是 Base64 则返回原字符串）
        String json = tryDecodeBase64(input);
        if (json == null) {
            log.error("Base64 解码失败");
            return null;
        }

        log.info("从命令行 JSON 字符串加载配置");
        return ConfigParser.parseFromString(json);
    }

    /**
     * 尝试解码 Base64 字符串
     * <p>
     * 如果输入是有效的 Base64 编码且解码后为 JSON 格式，返回解码后的字符串；
     * 否则返回原始字符串。
     *
     * @param input 待检测的字符串
     * @return 解码后的字符串，解码失败返回 null（仅当确定是 Base64 但解码失败时）
     */
    private static String tryDecodeBase64(String input) {
        String trimmed = input.trim();

        // 空字符串或太短的字符串不可能是有效的 Base64 编码的 JSON
        // 最短的 JSON {} 编码后为 e30= (4字符)
        if (trimmed.length() < 4) {
            return input;
        }

        // Base64 编码的字符串长度必须是 4 的倍数
        if (trimmed.length() % 4 != 0) {
            return input;
        }

        // 检查字符集是否符合 Base64 规范
        if (!BASE64_PATTERN.matcher(trimmed).matches()) {
            return input;
        }

        // 尝试解码，验证是否为有效的 Base64
        try {
            byte[] decoded = Base64.getDecoder().decode(trimmed);
            // 解码后的内容应该是 UTF-8 文本（JSON 字符串）
            String decodedString = new String(decoded, StandardCharsets.UTF_8);

            // 简单校验解码后的内容是否像 JSON（以 { 或 [ 开头）
            String trimmedDecoded = decodedString.trim();
            if (trimmedDecoded.startsWith("{") || trimmedDecoded.startsWith("[")) {
                log.info("检测到 Base64 编码的配置，已解码");
                log.debug("解码后的配置内容长度: {}", decodedString.length());
                return decodedString;
            }
            return input;
        } catch (IllegalArgumentException e) {
            // 不是有效的 Base64 编码，返回原始输入
            return input;
        }
    }
}