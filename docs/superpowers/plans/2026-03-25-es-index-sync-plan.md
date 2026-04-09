# ES Index Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一次性同步 NebulaGraph 数据到已存在的 ES 索引，处理索引数据缺失场景

**Architecture:** 复用 StorageScanner 遍历 NebulaGraph 数据，新增 ES 客户端写入数据，通过 SHOW FULLTEXT INDEXES 获取索引映射关系

**Tech Stack:** Go, nebula-go, net/http

---

## File Structure

| 文件 | 职责 |
|------|------|
| `service/es/client.go` | ES 客户端，连接管理，批量写入，重试机制 |
| `service/es/syncer.go` | 同步逻辑，DocID 生成，数据转换，字段过滤 |
| `service/es/client_test.go` | 单元测试 |
| `controllers/task.go` | 新增 SyncES handler |
| `routers/router.go` | 新增 `/api/task/sync-es` 路由 |

---

## Implementation Details

### 1. ES 地址获取

使用 `SHOW TEXT SEARCH CLIENTS` 获取 ES 地址（不是 SHOW LISTENER）：

```sql
nebula> SHOW TEXT SEARCH CLIENTS;
+-----------------+-----------------+------+
| Type            | Host            | Port |
+-----------------+-----------------+------+
| "ELASTICSEARCH" | "192.168.8.100" | 9200 |
+-----------------+-----------------+------+
```

### 2. ES 认证

API 新增参数：
```json
{
  "space": "test_space",
  "es_index": "person_idx",
  "batch_size": 1000,
  "es_username": "elastic",
  "es_password": "password123"
}
```

### 3. 字段过滤

只同步 `SHOW FULLTEXT INDEXES` 返回的 `Fields` 字段：

```go
// 解析 fields
fields := strings.Split(mapping.Fields, ",")
fieldSet := make(map[string]bool)
for _, f := range fields {
    f = strings.TrimSpace(f)
    if f != "" {
        fieldSet[f] = true
    }
}

// 只添加 mapping 中指定的字段
if fieldSet[k] {
    props[k] = val
}
```

### 4. DocID 生成

与 NebulaGraph 官方一致 (SHA256 hex)：

```go
func genDocID(vid, src, dst string, rank int64) string {
    var str string
    if vid != "" {
        str = vid
    } else {
        str = src + dst + fmt.Sprintf("%d", rank)
    }
    hash := sha256.Sum256([]byte(str))
    return hex.EncodeToString(hash[:])
}
```

### 5. 重试机制

- 最大重试次数: 6 次
- 初始间隔: 1 秒
- 退避策略: 指数退避（1s → 2s → 4s → 8s → 16s → 32s）
- 可重试错误: timeout, connection refused, 502, 503, 429 等

### 6. URL 处理

- 移除 Host 中的引号: `"192.168.8.100"` → `192.168.8.100`
- 添加 http:// 前缀: `192.168.8.100:9200` → `http://192.168.8.100:9200`

---

## API Usage

### Request

```bash
curl -X POST "http://127.0.0.1:8080/api/task/sync-es" \
  -H "Cookie: common-nsid=xxx" \
  -H "Content-Type: application/json" \
  -d '{
    "space": "test_space",
    "es_index": "person_idx",
    "batch_size": 1000,
    "es_username": "elastic",
    "es_password": "your_password"
  }'
```

### Response

```json
{
  "code": 0,
  "data": {
    "task_id": "xxx"
  },
  "message": "Sync task xxx submitted successfully"
}
```

### Check Task Status

```bash
curl -X POST "http://127.0.0.1:8080/api/task/copy/action" \
  -H "Cookie: common-nsid=xxx" \
  -H "Content-Type: application/json" \
  -d '{"taskID": "xxx", "taskAction": "status"}'
```

---

## 已完成的任务

### Task 1: 创建 ES 客户端 ✅

- [x] 创建 `service/es/client.go`
- [x] 使用 SHOW TEXT SEARCH CLIENTS 获取 ES 地址
- [x] 添加 ES 认证（Basic Auth）
- [x] 实现 BulkWrite 重试机制
- [x] 处理 URL 格式（移除引号、添加 http:// 前缀）

### Task 2: 实现同步逻辑 ✅

- [x] 创建 `service/es/syncer.go`
- [x] 实现 DocID 生成
- [x] 只同步 mapping.Fields 指定的字段
- [x] 实现 syncVertices 和 syncEdges

### Task 3: 新增 API 接口 ✅

- [x] 修改 `controllers/task.go`
- [x] 添加 `es_username`, `es_password` 参数
- [x] 修改 `routers/router.go`

### Task 4: 测试验证 ✅

- [x] 单元测试 `service/es/client_test.go`
- [x] 编译验证

---

## 常见错误排查

### 1. no text search clients configured

**原因:** NebulaGraph 未配置 ES client

**解决:**
```sql
ADD TEXT SEARCH CLIENT "192.168.8.100":9200;
```

### 2. no ES client found

**原因:** 返回的 Host 包含引号或格式不正确

**解决:** 代码已处理 `strings.ReplaceAll(host, "\"", "")`

### 3. first path segment in URL cannot contain colon

**原因:** URL 缺少 http:// 前缀

**解决:** 代码已自动添加 `http://` 前缀

### 4. bulk write failed after 6 retries

**原因:** ES 连接问题或认证失败

**排查:** 
- 检查 es_username/es_password 是否正确
- 检查 ES 地址是否可达
- 查看详细错误日志
