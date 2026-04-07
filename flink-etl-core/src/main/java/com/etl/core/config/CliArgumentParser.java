package com.etl.core.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StrSubstitutor;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
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
     * @return Job 配置
     */
    public static JobConfig parse(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        // 加载配置源为 JSON 字符串
        String json;
        if (params.has("file")) {
            json = loadFromFile(params.get("file"));
        } else if (params.has("config")) {
            json = loadFromJsonString(params.get("config"));
        } else {
            printUsage();
            throw new IllegalArgumentException("缺少必要参数：请指定 --file 或 --config");
        }

        // 将 Properties 转换为 Map<String, String>
        Properties properties = params.getProperties();
        Map<String, String> variables = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            variables.put(key, properties.getProperty(key));
        }

        // 统一进行变量替换
        String substitutedJson = substituteVariables(json, variables);
        checkUnresolvedVariables(substitutedJson);

        // 解析和校验 JSON
        return ConfigParser.parseFromString(substitutedJson);
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
     * 从文件加载配置为 JSON 字符串
     *
     * @param filePath 配置文件路径
     * @return JSON 字符串
     */
    private static String loadFromFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("--file 参数值不能为空");
        }

        if (!Files.exists(Paths.get(filePath))) {
            throw new IllegalArgumentException("配置文件不存在: " + filePath);
        }

        if (!new File(filePath).isFile()) {
            throw new IllegalArgumentException("路径不是文件: " + filePath);
        }

        log.info("从文件加载配置: {}", filePath);
        return readFileContent(filePath);
    }

    /**
     * 从命令行参数加载配置为 JSON 字符串
     * 支持 JSON 字符串或 Base64 编码
     *
     * @param input JSON 字符串或 Base64 编码
     * @return JSON 字符串
     */
    private static String loadFromJsonString(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("--config 参数值不能为空");
        }

        String json = tryDecodeBase64(input);
        log.info("从命令行 JSON 字符串加载配置");
        return json;
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

    /**
     * 使用 StrSubstitutor 替换配置中的变量
     *
     * 支持格式：
     * - ${variable} - 变量不存在时保留占位符
     * - ${variable:-default} - 变量不存在时使用默认值
     *
     * @param json JSON 配置字符串
     * @param variables 变量映射（从 ParameterTool.getProperties() 获取）
     * @return 替换后的 JSON 字符串
     */
    private static String substituteVariables(String json, Map<String, String> variables) {
        StrSubstitutor substitutor = new StrSubstitutor(variables);
        return substitutor.replace(json);
    }

    /**
     * 检查 JSON 字符串中是否存在未替换的变量占位符
     *
     * 严格模式：发现任何 ${...} 格式的占位符都会抛出异常
     *
     * @param json 替换后的 JSON 字符串
     * @throws IllegalArgumentException 如果存在未替换的变量
     */
    private static void checkUnresolvedVariables(String json) {
        Pattern pattern = Pattern.compile("\\$\\{[^}]+\\}");
        Matcher matcher = pattern.matcher(json);

        if (matcher.find()) {
            String unresolvedVar = matcher.group();
            // 提取变量名（去掉 ${ 和 }）
            String varName = unresolvedVar.substring(2, unresolvedVar.length() - 1);
            // 提取实际变量名（去掉默认值部分 :-default）
            String actualVarName = varName.split(":-")[0];

            throw new IllegalArgumentException(
                String.format("配置变量替换失败：变量 '%s' 未定义，请通过 --%s 参数传递",
                    actualVarName, actualVarName)
            );
        }
    }

    /**
     * 读取文件内容为字符串
     *
     * @param filePath 文件路径
     * @return 文件内容
     * @throws IllegalArgumentException 读取失败时抛出
     */
    private static String readFileContent(String filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("读取配置文件失败: " + e.getMessage(), e);
        }
    }
}