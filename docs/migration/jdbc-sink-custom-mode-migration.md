# JDBC Sink Custom 模式迁移指南

## 迁移影响

### 需要迁移的配置场景

| 原配置 | 新配置 | 迁移步骤 |
|--------|--------|---------|
| `mode=insert, sql=...` | `mode=custom, sql=...` | 修改 mode 参数为 custom |
| `mode=upsert, sql=INSERT ... ON DUPLICATE KEY UPDATE` | `mode=custom, sql=...` | 修改 mode 参数为 custom |

### 迁移示例

**场景 1：原 INSERT 模式使用自定义 SQL**

```json
// 旧配置（会报错）
{
  "sink": {
    "config": {
      "mode": "insert",
      "sql": "INSERT INTO users(id, name) VALUES(:id, :name)"
    }
  }
}

// 新配置
{
  "sink": {
    "config": {
      "mode": "custom",
      "sql": "INSERT INTO users(id, name) VALUES(:id, :name)"
    }
  }
}
```

**场景 2：原 UPSERT 模式使用自定义 SQL**

```json
// 旧配置（会报错）
{
  "sink": {
    "config": {
      "mode": "upsert",
      "sql": "INSERT INTO users(id, name) VALUES(:id, :name) ON DUPLICATE KEY UPDATE name=VALUES(name)"
    }
  }
}

// 新配置
{
  "sink": {
    "config": {
      "mode": "custom",
      "sql": "INSERT INTO users(id, name) VALUES(:id, :name) ON DUPLICATE KEY UPDATE name=VALUES(name)"
    }
  }
}
```

## 错误提示

```
INSERT 模式必须配置 table
UPSERT 模式必须配置 table
CDC 模式必须配置 table
CUSTOM 模式必须配置 sql
表 'users' 没有主键，无法使用 UPSERT 模式。请手动配置 keyFields 或为表添加主键
```