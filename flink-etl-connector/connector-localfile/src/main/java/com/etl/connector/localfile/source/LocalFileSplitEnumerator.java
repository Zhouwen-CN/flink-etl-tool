package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.core.exception.SourceConfigException;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 本地文件分片枚举器
 * 继承 AbstractSplitEnumerator，自动处理分片分配逻辑
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
public class LocalFileSplitEnumerator extends AbstractSplitEnumerator<LocalFileSplit> {

    private final LocalFileSourceConfig localFileSourceConfig;

    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            LocalFileSourceConfig localFileSourceConfig) {
        super(context);
        this.localFileSourceConfig = localFileSourceConfig;
    }

    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            BaseEnumCheckpoint<LocalFileSplit> checkpoint,
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

        List<File> matchedFiles = scanFiles(pathPattern, recursive);
        if (matchedFiles.isEmpty()) {
            throw new SourceConfigException("未找到匹配的文件: " + pathPattern);
        }

        log.info("找到 {} 个匹配的文件", matchedFiles.size());

        List<LocalFileSplit> splits = new ArrayList<>();
        for (File file : matchedFiles) {
            splits.add(new LocalFileSplit(file.getAbsolutePath(), localFileSourceConfig));
        }

        addPendingSplits(splits);
        log.info("创建了 {} 个文件分片", splits.size());
    }

    private List<File> scanFiles(String pathPattern, boolean recursive) {
        List<File> result = new ArrayList<>();

        Path basePath = getBasePath(pathPattern);
        String globPattern = getGlobPattern(pathPattern);

        log.debug("基础路径: {}, glob 模式: {}", basePath, globPattern);

        if (basePath == null) {
            throw new SourceConfigException("无法确定文件扫描的基础路径: " + pathPattern);
        }

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);

        try {
            if (recursive) {
                try (Stream<Path> pathStream = Files.walk(basePath)) {
                    pathStream.filter(Files::isRegularFile)
                            .filter(path -> matcher.matches(basePath.relativize(path)))
                            .forEach(path -> result.add(path.toFile()));
                }
            } else {
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

    private Path getBasePath(String pathPattern) {
        int wildcardIndex = findWildcardIndex(pathPattern);

        if (wildcardIndex == -1) {
            Path parent = Paths.get(pathPattern).getParent();
            return parent != null ? parent : Paths.get(".");
        }

        String basePath = pathPattern.substring(0, wildcardIndex);
        Path path = Paths.get(basePath);

        if (basePath.endsWith("/") || basePath.endsWith("\\")) {
            return path;
        }

        return path.getParent() != null ? path.getParent() : Paths.get(".");
    }

    private String getGlobPattern(String pathPattern) {
        int wildcardIndex = findWildcardIndex(pathPattern);

        if (wildcardIndex == -1) {
            return Paths.get(pathPattern).getFileName().toString();
        }

        int lastSeparatorIndex = pathPattern.lastIndexOf('/', wildcardIndex);
        if (lastSeparatorIndex == -1) {
            lastSeparatorIndex = pathPattern.lastIndexOf('\\', wildcardIndex);
        }

        return pathPattern.substring(lastSeparatorIndex + 1);
    }

    @Override
    public void close() throws IOException {
        log.info("LocalFileSplitEnumerator 关闭");
    }
}
