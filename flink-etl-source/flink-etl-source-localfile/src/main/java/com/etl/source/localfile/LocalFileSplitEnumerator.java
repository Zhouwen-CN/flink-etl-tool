package com.etl.source.localfile;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.base.BaseSplitEnumerator;
import com.etl.source.localfile.format.FileFormatPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * 本地文件分片枚举器
 * 继承 BaseSplitEnumerator，自动处理分片分配逻辑
 *
 * <p>职责：
 * <ul>
 *   <li>扫描文件系统，匹配通配符路径</li>
 *   <li>调用 FileFormatPlugin 解析字段名</li>
 *   <li>创建文件分片并分配给 Reader</li>
 * </ul>
 */
@Slf4j
public class LocalFileSplitEnumerator extends BaseSplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint> {

    private final String pathPattern;
    private final boolean recursive;
    private final String format;
    private final SourceConfig config;

    private List<String> fields;

    /**
     * 构造函数
     *
     * @param context 枚举器上下文
     * @param config 配置
     * @param format 格式类型
     */
    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            SourceConfig config,
            String format) {
        super(context);
        this.config = config;
        this.pathPattern = config.getString("path");
        this.recursive = config.getBoolean("recursive", false);
        this.format = format;
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context 枚举器上下文
     * @param checkpoint 检查点
     * @param config 配置
     * @param format 格式类型
     */
    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            LocalFileEnumCheckpoint checkpoint,
            SourceConfig config,
            String format) {
        super(context, checkpoint);
        this.config = config;
        this.pathPattern = config.getString("path");
        this.recursive = config.getBoolean("recursive", false);
        this.format = format;
    }

    @Override
    public void start() {
        log.info("LocalFileSplitEnumerator 启动，路径模式: {}", pathPattern);

        // 扫描匹配的文件
        List<File> matchedFiles = scanFiles();
        if (matchedFiles.isEmpty()) {
            throw new RuntimeException("未找到匹配的文件: " + pathPattern);
        }

        log.info("找到 {} 个匹配的文件", matchedFiles.size());

        // 解析字段名（使用第一个文件）
        resolveFields(matchedFiles.get(0));

        // 创建分片
        List<LocalFileSplit> splits = new ArrayList<>();
        for (File file : matchedFiles) {
            splits.add(new LocalFileSplit(file.getAbsolutePath(), fields));
        }

        // 添加到待处理队列
        addPendingSplits(splits);
        log.info("创建了 {} 个文件分片", splits.size());
    }

    /**
     * 扫描匹配的文件
     */
    private List<File> scanFiles() {
        List<File> result = new ArrayList<>();

        // 解析路径模式
        Path basePath = getBasePath(pathPattern);
        String globPattern = getGlobPattern(pathPattern);

        log.debug("基础路径: {}, glob 模式: {}", basePath, globPattern);

        // 创建 PathMatcher
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);

        try {
            if (recursive) {
                // 递归遍历
                Files.walk(basePath)
                        .filter(Files::isRegularFile)
                        .filter(path -> matcher.matches(basePath.relativize(path)))
                        .forEach(path -> result.add(path.toFile()));
            } else {
                // 非递归遍历
                Files.list(basePath)
                        .filter(Files::isRegularFile)
                        .filter(path -> matcher.matches(basePath.relativize(path)))
                        .forEach(path -> result.add(path.toFile()));
            }
        } catch (IOException e) {
            throw new RuntimeException("扫描文件失败: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * 获取基础路径（去除通配符部分）
     */
    private Path getBasePath(String pathPattern) {
        // 找到第一个通配符的位置
        int wildcardIndex = pathPattern.indexOf('*');
        if (wildcardIndex == -1) {
            wildcardIndex = pathPattern.indexOf('?');
        }

        if (wildcardIndex == -1) {
            // 没有通配符，直接返回路径
            return Paths.get(pathPattern).getParent();
        }

        // 截取通配符之前的部分
        String basePath = pathPattern.substring(0, wildcardIndex);
        Path path = Paths.get(basePath);

        // 如果路径以分隔符结尾，获取其父目录
        if (basePath.endsWith("/") || basePath.endsWith("\\")) {
            return path;
        }

        return path.getParent() != null ? path.getParent() : Paths.get(".");
    }

    /**
     * 获取 glob 模式（相对于基础路径）
     */
    private String getGlobPattern(String pathPattern) {
        // 找到第一个通配符的位置
        int wildcardIndex = pathPattern.indexOf('*');
        if (wildcardIndex == -1) {
            wildcardIndex = pathPattern.indexOf('?');
        }

        if (wildcardIndex == -1) {
            // 没有通配符，匹配文件名
            return Paths.get(pathPattern).getFileName().toString();
        }

        // 找到通配符之前的最后一个路径分隔符
        int lastSeparatorIndex = pathPattern.lastIndexOf('/', wildcardIndex);
        if (lastSeparatorIndex == -1) {
            lastSeparatorIndex = pathPattern.lastIndexOf('\\', wildcardIndex);
        }

        return pathPattern.substring(lastSeparatorIndex + 1);
    }

    /**
     * 解析字段名
     */
    private void resolveFields(File firstFile) {
        // 动态加载格式插件
        FileFormatPlugin formatPlugin = loadFormatPlugin(format);

        try (InputStream inputStream = new FileInputStream(firstFile)) {
            fields = formatPlugin.resolveFields(config, inputStream);
            log.info("解析到 {} 个字段: {}", fields.size(), fields);
        } catch (IOException e) {
            throw new RuntimeException("解析字段名失败: " + e.getMessage(), e);
        }
    }

    /**
     * 加载格式插件
     */
    private FileFormatPlugin loadFormatPlugin(String format) {
        ServiceLoader<FileFormatPlugin> loader = ServiceLoader.load(FileFormatPlugin.class);
        for (FileFormatPlugin plugin : loader) {
            if (plugin.getType().equalsIgnoreCase(format)) {
                log.info("加载格式插件: {}", plugin.getClass().getName());
                return plugin;
            }
        }
        throw new RuntimeException("未找到格式插件: " + format);
    }

    /**
     * 获取字段名列表
     */
    public List<String> getFields() {
        return fields;
    }

    @Override
    public LocalFileEnumCheckpoint snapshotState(long checkpointId) {
        List<LocalFileSplit> pending = List.copyOf(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new LocalFileEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("LocalFileSplitEnumerator 关闭");
    }
}