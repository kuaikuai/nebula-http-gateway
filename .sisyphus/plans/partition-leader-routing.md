# 按分区访问不同 Storage 服务地址计划

## TL;DR

> 实现每个分区连接到正确的 Storage 服务地址，通过 `SHOW PARTS` 获取分区 Leader 映射。

## Context

### 当前问题
- 当前只连接到一个 storage 地址
- 所有分区扫描请求都发往同一个地址
- Nebula Graph 中不同分区可能分布在不同的 storage 节点上

### 解决方案
通过 `SHOW PARTS` 命令获取分区 Leader 信息，每个分区向对应的 Leader 发送请求。

---

## Work Objectives

### Core Objective
1. 从 `SHOW PARTS` 获取分区 Leader 映射
2. 为每个分区维护正确的 storage 地址
3. 扫描时向正确的 Storage 节点发送请求

### Definition of Done
- [ ] 从 SHOW PARTS 获取分区 Leader 映射
- [ ] 为每个分区创建对应的 Storage Client
- [ ] 扫描时使用对应分区的 Client

---

## 实现方案

### 1. 新增 PartitionInfo 结构

```go
// PartitionInfo holds partition leader information
type PartitionInfo struct {
    PartID nebula.PartitionID
    Leader string  // host:port
}
```

### 2. 修改 getPartitionIDs 为 getPartitionInfo

```go
func (s *StorageScanner) getPartitionInfo() ([]PartitionInfo, error) {
    result, _, err := dao.Execute(s.nsid, fmt.Sprintf("USE %s; SHOW PARTS", s.spaceName), nil)
    if err != nil {
        return nil, err
    }

    var parts []PartitionInfo
    for _, row := range result.Tables {
        if partID, ok := row["Partition ID"].(int64); ok {
            var leader string
            if leaderVal, ok := row["Leader"]; ok {
                leader = fmt.Sprintf("%v", leaderVal)
            }
            parts = append(parts, PartitionInfo{
                PartID: nebula.PartitionID(partID),
                Leader: leader,
            })
        }
    }
    return parts, nil
}
```

### 3. 修改 StorageScanner 结构

```go
type StorageScanner struct {
    // ... existing fields
    partInfo      []PartitionInfo
    storageClients map[string]*storage.GraphStorageServiceClient  // addr -> client
}
```

### 4. 修改 NewStorageScanner 初始化

```go
// 获取分区信息（包括 Leader）
partInfo, err := scanner.getPartitionInfo()
scanner.partInfo = partInfo

// 收集所有唯一的 Leader 地址
uniqueAddrs := make(map[string]bool)
for _, p := range partInfo {
    if p.Leader != "" {
        uniqueAddrs[p.Leader] = true
    }
}

// 为每个地址创建 client
scanner.storageClients = make(map[string]*storage.GraphStorageServiceClient)
for addr := range uniqueAddrs {
    client, err := createStorageClient(addr)
    if err != nil {
        return nil, err
    }
    scanner.storageClients[addr] = client
}
```

### 5. 修改 createStorageClient 为工厂方法

```go
func createStorageClient(addr string) (*storage.GraphStorageServiceClient, error) {
    parts := strings.Split(strings.TrimSpace(addr), ":")
    // ... existing logic
}
```

### 6. 修改 ScanVertices/ScanEdges

```go
// 初始化分区状态时，同时记录 Leader
partStates := make(map[nebula.PartitionID]*partState)
for _, p := range s.partInfo {
    partStates[p.PartID] = &partState{leader: p.Leader}
}

// 扫描时使用对应分区的 client
for pid, state := range partStates {
    if state.done { continue }
    
    client := s.storageClients[state.leader]
    resp, err := client.ScanVertex(req)
}
```

### 7. 修改 partState 结构

```go
type partState struct {
    cursor   *storage.ScanCursor
    done     bool
    leader   string  // 新增：该分区对应的 storage 地址
}
```

---

## 代码位置修改

| 位置 | 修改内容 |
|------|---------|
| `PartitionInfo` 结构体 | 新增 |
| `getPartitionInfo()` | 替代 `getPartitionIDs()`，同时获取 Leader |
| `StorageScanner` 结构体 | 添加 `partInfo`, `storageClients` 字段 |
| `NewStorageScanner` | 初始化分区信息和 clients |
| `createStorageClient(addr)` | 改为工厂方法，参数为地址 |
| `partState` 结构体 | 添加 `leader` 字段 |
| `ScanVertices/ScanEdges` | 使用对应分区的 client |

---

## 不需要处理的情况

- ❌ E_LEADER_CHANGED 错误
- ❌ 连接池

---

## Commit Strategy

- **1**: `refactor(scanner): route scan requests to partition leader addresses`
  - Files: `service/copier/storage_scanner.go`
