package com.etl.source.localfile;

import com.etl.core.exception.SourceConfigException;
import com.etl.core.source.BaseSplitEnumerator;
import com.etl.source.localfile.config.LocalFileSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 本地文件分片枚举器
 * 继承 BaseSplitEnumerator，自动处理分片分配逻辑
 *
 * <p>职责：
 * <ul>
 *   <li>扫描文件系统，匹配通配符路径</li>
 *   <li>创建文件分片并分配给 Reader</li>
 * </ul>
 *
 * <p>注意：字段名和类型从 source.schema 配置中获取，无需从文件推断
 */
@Slf4j
public class LocalFileSplitEnumerator extends BaseSplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint> {

    private final LocalFileSourceConfig localFileSourceConfig;

    /**
     * 构造函数
     *
     * @param context               枚举器上下文
     * @param localFileSourceConfig 配置
     */
    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            LocalFileSourceConfig localFileSourceConfig) {
        super(context);
        this.localFileSourceConfig = localFileSourceConfig;
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context               枚举器上下文
     * @param checkpoint            检查点
     * @param localFileSourceConfig 配置
     */
    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            LocalFileEnumCheckpoint checkpoint,
            LocalFileSourceConfig localFileSourceConfig) {
        super(context, checkpoint);
        this.localFileSourceConfig = localFileSourceConfig;
    }

    /**
     * 查找路径中第一个通配符（*）的位置
     * 支持 * 和 ** 通配符，不支持 ? 通配符
     */
    private static int findWildcardIndex(String pathPattern) {
        return pathPattern.indexOf('*');
    }

    @Override
    public void start() {
        String pathPattern = localFileSourceConfig.getPathPattern();
        boolean recursive = localFileSourceConfig.isRecursive();
        log.info("LocalFileSplitEnumerator 启动，路径模式: {}，是否递归: {}", pathPattern, recursive);

        // 扫描匹配的文件
        List<File> matchedFiles = scanFiles(pathPattern, recursive);
        if (matchedFiles.isEmpty()) {
            throw new SourceConfigException("未找到匹配的文件: " + pathPattern);
        }

        log.info("找到 {} 个匹配的文件", matchedFiles.size());

        // 创建分片
        List<LocalFileSplit> splits = new ArrayList<>();
        for (File file : matchedFiles) {
            splits.add(new LocalFileSplit(file.getAbsolutePath()));
        }

        // 添加到待处理队列
        addPendingSplits(splits);
        log.info("创建了 {} 个文件分片", splits.size());
    }

    /**
     * 扫描匹配的文件
     */
    private List<File> scanFiles(String pathPattern, boolean recursive) {
        List<File> result = new ArrayList<>();

        // 解析路径模式
        Path basePath = getBasePath(pathPattern);
        String globPattern = getGlobPattern(pathPattern);

        log.debug("基础路径: {}, glob 模式: {}", basePath, globPattern);

        if (basePath == null) {
            throw new SourceConfigException("无法确定文件扫描的基础路径: " + pathPattern);
        }

        // 创建 PathMatcher
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);

        try {
            if (recursive) {
                // 递归遍历
                try (Stream<Path> pathStream = Files.walk(basePath)) {
                    pathStream.filter(Files::isRegularFile)
                            .filter(path -> matcher.matches(basePath.relativize(path)))
                            .forEach(path -> result.add(path.toFile()));
                }
            } else {
                // 非递归遍历
                try (Stream<Path> pathStream = Files.list(basePath)) {
                    pathStream.filter(Files::isRegularFile)
                            .filter(path -> matcher.matches(basePath.relativize(path)))
                            .forEach(path -> result.add(path.toFile()));
                }
            }
        } catch (IOException e) {
            throw new SourceConfigException("扫描文件失败: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * 获取基础路径（去除通配符部分）
     */
    private Path getBasePath(String pathPattern) {
        // 找到第一个通配符的位置
        int wildcardIndex = findWildcardIndex(pathPattern);

        if (wildcardIndex == -1) {
            // 没有通配符，返回路径的父目录，为 null 时使用当前目录
            Path parent = Paths.get(pathPattern).getParent();
            return parent != null ? parent : Paths.get(".");
        }

        // 截取通配符之前的部分
        String basePath = pathPattern.substring(0, wildcardIndex);
        Path path = Paths.get(basePath);

        // 如果路径以分隔符结尾，返回该路径
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
        int wildcardIndex = findWildcardIndex(pathPattern);

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

    @Override
    public LocalFileEnumCheckpoint snapshotState(long checkpointId) {
        List<LocalFileSplit> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new LocalFileEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("LocalFileSplitEnumerator 关闭");
    }
}