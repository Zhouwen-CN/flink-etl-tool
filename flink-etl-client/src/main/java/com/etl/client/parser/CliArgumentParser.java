package com.etl.client.parser;

import com.etl.core.config.JobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
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
            json = loadFromString(params.get("config"));
        } else {
            throw new IllegalArgumentException("缺少必要参数：请指定 --file 或 --config");
        }

        // 统一进行变量替换（仅当配置中包含变量占位符时）
        String substitutedJson = json;
        if (json.contains("${")) {
            substitutedJson = StrSubstitutor.replace(json, params.getProperties());
            checkUnresolvedVariables(substitutedJson);
        }

        // 解析和校验 JSON
        return ConfigParser.parse(substitutedJson);
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

        Path path = Paths.get(filePath);
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("文件不存在: " + filePath);
        }

        log.info("从文件加载配置: {}", filePath);

        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("读取配置文件失败: " + e.getMessage(), e);
        }
    }

    /**
     * 从命令行参数加载配置为 JSON 字符串
     * 支持 JSON 字符串或 Base64 编码
     *
     * @param input JSON 字符串或 Base64 编码
     * @return JSON 字符串
     */
    private static String loadFromString(String input) {
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
            if (trimmedDecoded.startsWith("{") && trimmedDecoded.endsWith("}")) {
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
     * 检查 JSON 字符串中是否存在未替换的变量占位符
     * <p>
     * 严格模式：发现任何 ${...} 格式的占位符都会抛出异常
     *
     * @param json 替换后的 JSON 字符串
     * @throws IllegalArgumentException 如果存在未替换的变量
     */
    private static void checkUnresolvedVariables(String json) {
        Pattern pattern = Pattern.compile("\\$\\{[^}]+}");
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
}