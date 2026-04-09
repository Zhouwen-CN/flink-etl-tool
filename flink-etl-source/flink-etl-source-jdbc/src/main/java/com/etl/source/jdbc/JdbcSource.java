package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.core.utils.SqlUtils;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import com.etl.source.jdbc.enums.SplitStrategy;
import com.etl.source.jdbc.utils.JdbcSplitHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
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
                        String.format("分片列 '%s' 的 JDBC 类型(%d)不支持分片。支持的类型: %s",
                                userSplitKey, jdbcType, SplitStrategy.NUMERIC.getSupportedTypeNames()));
            }
            log.info("分片列 '{}' 使用策略: {}", userSplitKey, strategy.getDescription());
            return Pair.of(userSplitKey, strategy);
        }

        // 2. 配置了 table → 自动从主键推断
        if (table != null) {
            try {
                Map<String, Integer> primaryKeys = SqlUtils.getPrimaryKey(url, table, username, password);

                if (primaryKeys.isEmpty()) {
                    throw new NoPrimaryKeyException(table);
                }

                // 从主键中选择最优的 splitKey
                String optimalKey = selectOptimalSplitKey(primaryKeys);
                if (optimalKey != null) {
                    int jdbcType = primaryKeys.get(optimalKey);
                    SplitStrategy strategy = SplitStrategy.fromJdbcType(jdbcType);
                    log.info("自动推断分片列 '{}' (类型: {}), 使用策略: {}",
                            optimalKey, getJdbcTypeName(jdbcType), strategy.getDescription());
                    return Pair.of(optimalKey, strategy);
                } else {
                    // 主键列类型都不支持，降级为单分片模式
                    log.warn("表 '{}' 的主键列类型都不支持分片，将使用单分片全表扫描模式。支持的类型: {}",
                            table, SplitStrategy.NUMERIC.getSupportedTypeNames());
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

    /**
     * 从复合主键中选择最优的 splitKey
     * 优先级：BIGINT > INTEGER > SMALLINT > TINYINT > DECIMAL/NUMERIC > FLOAT/REAL/DOUBLE
     *
     * @param primaryKeys 主键列及其 JDBC 类型（LinkedHashMap 保证顺序）
     * @return 最优的列名，如果所有列类型都不支持则返回 null
     */
    private String selectOptimalSplitKey(Map<String, Integer> primaryKeys) {
        String selectedKey = null;
        int selectedPriority = -1;

        // 类型优先级定义（值越大优先级越高）
        Map<Integer, Integer> typePriority = new LinkedHashMap<>();
        typePriority.put(Types.BIGINT, 6);
        typePriority.put(Types.INTEGER, 5);
        typePriority.put(Types.SMALLINT, 4);
        typePriority.put(Types.TINYINT, 3);
        typePriority.put(Types.DECIMAL, 2);
        typePriority.put(Types.NUMERIC, 2);
        typePriority.put(Types.FLOAT, 1);
        typePriority.put(Types.REAL, 1);
        typePriority.put(Types.DOUBLE, 1);

        for (Map.Entry<String, Integer> entry : primaryKeys.entrySet()) {
            String columnName = entry.getKey();
            int jdbcType = entry.getValue();
            Integer priority = typePriority.get(jdbcType);

            if (priority != null && priority > selectedPriority) {
                selectedKey = columnName;
                selectedPriority = priority;
            }
        }

        return selectedKey;
    }

    /**
     * 获取 JDBC 类型的名称（用于日志输出）
     */
    private String getJdbcTypeName(int jdbcType) {
        switch (jdbcType) {
            case Types.BIGINT:
                return "BIGINT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.DOUBLE:
                return "DOUBLE";
            default:
                return String.valueOf(jdbcType);
        }
    }

}