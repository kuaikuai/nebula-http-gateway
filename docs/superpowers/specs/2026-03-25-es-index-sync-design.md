# ES Index Sync Design

**Date:** 2026-03-25
**Status:** Draft

## Overview

一次性同步 NebulaGraph 数据到已存在的 ES 索引，处理索引数据缺失场景。

## API Design

### Endpoint
```
POST /api/task/sync-es
```

### Request
```json
{
  "space": "test_space",
  "es_index": "person_idx",
  "batch_size": 1000
}
```

### Response
```json
{
  "code": 0,
  "data": {
    "task_id": "xxx",
    "indexed_count": 10000
  },
  "message": "Sync task submitted"
}
```

## ES Doc Structure

### Mapping (与 NebulaGraph 官方一致)
```json
{
  "mappings": {
    "properties": {
      "vid": { "type": "keyword" },
      "src": { "type": "keyword" },
      "dst": { "type": "keyword" },
      "rank": { "type": "long" },
      "属性名": { "type": "text", "analyzer": "standard" }
    }
  }
}
```

### DocID Generation (与 NebulaGraph 官方一致)
```cpp
// C++ 源码逻辑
if (!vid.empty()) {
    docID = SHA256(vid)
} else {
    docID = SHA256(src + dst + rank)
}
```

### 文档示例
```json
{
  "vid": "tom",
  "src": "",
  "dst": "",
  "rank": 0,
  "name": "Tom",
  "age": 30
}
```

## Flow

1. **获取 ES 配置**
   - 调用 SHOW LISTENER 获取 ES 地址

2. **获取索引映射**
   - 调用 SHOW FULLTEXT INDEXES 获取索引 → Tag/Edge 映射

3. **遍历数据**
   - 使用 StorageScanner 遍历 NebulaGraph 数据

4. **写入 ES**
   - 批量写入 ES

## Components

### New Files
- `service/es/client.go` - ES 客户端
- `service/es/syncer.go` - 同步逻辑

### Modified Files
- `controllers/task.go` - 新增 SyncES handler
- `routers/router.go` - 新增路由

## Error Handling

- ES 连接失败：返回错误
- 索引不存在：返回错误
- 批量写入失败：重试机制
