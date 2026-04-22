package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.dialect.JdbcDialectLoader;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import com.etl.connector.jdbc.utils.JdbcSplitHelper;
import com.etl.core.utils.SqlUtils;
import com.etl.core.config.SourceConfig;
import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSource(SourceConfig config) {
        super(config);
        String url = Preconditions.checkNotNull(config.getString("url"), "url is null");

        // 支持显式配置 dialect
        String dialectName = config.getString("dialect");
        JdbcDialect dialect = JdbcDialectLoader.get(dialectName, url);

        // 使用 Dialect 包装 URL
        url = dialect.wrapUrl(url);

        String username = config.getString("username");
        String password = config.getString("password");

        String table = config.getString("table");
        String sql = config.getString("sql");

        // 自动推断 splitKey 和 splitStrategy
        Pair<String, SplitStrategy> inferred = inferSplitKey(
                config.getString("splitKey"), table, sql, url, username, password, dialect);
        String splitKey = inferred.getLeft();
        SplitStrategy splitStrategy = inferred.getRight();

        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        Integer queryTimeout = config.getInteger("queryTimeout");

        this.jdbcSourceConfig = JdbcSourceConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitKey(splitKey)
                .splitStrategy(splitStrategy)
                .batchSize(batchSize)
                .queryTimeout(queryTimeout)
                .dialect(dialect)
                .build();

        log.info("创建 JdbcSource: {}", this.jdbcSourceConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      RangeEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, checkpoint, jdbcSourceConfig);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<BaseSplitReader<Row, RangeSplit>> splitReaderSupplier = () ->
                new JdbcSplitReader(jdbcSourceConfig);
        return new JdbcSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<RangeEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();

        return SqlUtils.inferRowType(
                table,
                sql,
                url,
                username,
                password
        );
    }

    /**
     * 推断 splitKey 和 splitStrategy
     *
     * @param userSplitKey 用户配置的 splitKey（可选）
     * @param table        表名（可选）
     * @param sql          SQL 语句（可选）
     * @param url          JDBC URL
     * @param username     用户名
     * @param password     密码
     * @param dialect      JDBC 方言
     * @return Pair&lt;splitKey, splitStrategy&gt;，splitKey 可能为 null（单分片模式）
     */
    private Pair<String, SplitStrategy> inferSplitKey(
            String userSplitKey, String table, String sql,
            String url, String username, String password, JdbcDialect dialect) {

        // 参数校验：table 和 sql 至少配置一个
        Preconditions.checkArgument(table != null || sql != null,
                "table 和 sql 至少配置一个");

        // 1. 用户配置了 splitKey → 验证类型
        if (userSplitKey != null) {
            int jdbcType = JdbcSplitHelper.getColumnType(dialect, url, table, sql, userSplitKey, username, password);
            SplitStrategy strategy = SplitStrategy.fromJdbcType(jdbcType);
            if (strategy == null) {
                throw new IllegalArgumentException(
                        String.format("分片列 '%s' 的 JDBC 类型(%s)不支持分片。", userSplitKey, JdbcSplitHelper.getJdbcTypeName(jdbcType))
                );
            }
            log.info("分片列 '{}' 使用策略: {}", userSplitKey, strategy.getDescription());
            return Pair.of(userSplitKey, strategy);
        }

        // 2. 配置了 table → 自动从主键推断
        if (table != null) {
            try {
                List<Pair<String, Integer>> primaryKeys = SqlUtils.getPrimaryKey(url, table, username, password);

                // 最左前缀原则
                Pair<String, Integer> pair = primaryKeys.get(0);
                String firstPrimaryKey = pair.getKey();
                Integer jdbcType = pair.getValue();
                SplitStrategy strategy = SplitStrategy.fromJdbcType(jdbcType);

                if (strategy != null) {
                    log.info("自动推断分片列 '{}' (类型: {}), 使用策略: {}",
                            firstPrimaryKey, JdbcSplitHelper.getJdbcTypeName(jdbcType), strategy.getDescription());
                    return Pair.of(firstPrimaryKey, strategy);
                } else {
                    // 主键列类型都不支持，降级为单分片模式
                    log.warn("表 '{}' 的主键列类型不支持分片，将使用单分片全表扫描模式。", table);
                    return Pair.of(null, SplitStrategy.FULL_TABLE_SCAN);
                }
            } catch (NoPrimaryKeyException e) {
                throw new RuntimeException(
                        String.format("无法自动推断 splitKey: %s。请显式配置 splitKey 参数或确保表有主键。",
                                e.getMessage()));
            }
        }

        // 3. 配置了 sql（无 table）→ 单分片模式
        log.warn("使用 SQL 查询且未配置 splitKey，将使用单分片全表扫描模式，无法并行读取。建议配置 splitKey 以启用并行分片读取。");
        return Pair.of(null, SplitStrategy.FULL_TABLE_SCAN);
    }

}