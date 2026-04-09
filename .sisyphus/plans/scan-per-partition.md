# ScanVertices / ScanEdges 逐分区扫描改造计划

## TL;DR

> 将 ScanVertices 和 ScanEdges 改为逐分区扫描，独立维护每个分区的 cursor 和完成状态，参考 Java client 实现。

## Context

### 当前问题
1. 一次性发送所有分区，导致 cursor 管理混乱
2. `resp.GetCursors()` 只返回有数据的分区，导致提前退出
3. 无法区分哪些分区已完成、哪些还有数据

### 参考实现
Java client 每次只扫描1个分区，独立维护每个分区的 cursor 和完成状态。

---

## Work Objectives

### Core Objective
1. **逐分区扫描**：每次只扫描1个分区
2. **独立状态跟踪**：为每个分区维护独立的 cursor 和完成状态
3. **完整遍历**：确保所有分区都被处理

### Concrete Deliverables
- 修改 `ScanVertices` 函数
- 修改 `ScanEdges` 函数
- 可能需要添加辅助结构体

### Definition of Done
- [ ] ScanVertices 逐分区扫描
- [ ] ScanEdges 逐分区扫描
- [ ] 每个分区独立维护 cursor
- [ ] 遍历完所有分区才结束

---

## 修改方案

### 核心思路

**数据结构**：为每个分区维护独立状态
```go
type partState struct {
    cursor   *storage.ScanCursor
    done     bool
}

// 初始化所有分区状态
partStates := make(map[nebula.PartitionID]*partState)
for _, pid := range s.partIDs {
    partStates[pid] = &partState{cursor: nil, done: false}
}
```

**扫描逻辑**：
```
循环直到所有分区完成:
    for _, pid := range s.partIDs {
        state := partStates[pid]
        if state.done {
            continue  // 跳过已完成的分区
        }
        
        // 扫描当前分区
        req.Parts = map[pid]state.cursor
        
        // 处理响应
        // ...
        
        // 更新状态
        if cursor == nil || 没有更多数据 {
            state.done = true
        } else {
            state.cursor = cursor
        }
    }
    
    // 检查是否全部完成
    allDone := true
    for _, state := range partStates {
        if !state.done {
            allDone = false
            break
        }
    }
    if allDone {
        break
    }
```

### 具体修改

#### 1. ScanVertices

**当前逻辑** (有问题):
```go
parts := make(map[nebula.PartitionID]*storage.ScanCursor)
for {
    req := &storage.ScanVertexRequest{
        Parts: parts,  // 发送所有分区
    }
    resp := ...
    parts = resp.GetCursors()  // 替换，丢失已完成分区信息
}
```

**修改后**:
```go
// 初始化分区状态
partStates := make(map[nebula.PartitionID]*partState)
for _, pid := range s.partIDs {
    partStates[pid] = &partState{}
}

for {
    hasData := false
    
    // 逐分区扫描
    for pid, state := range partStates {
        if state.done {
            continue
        }
        
        // 构建单分区请求
        reqParts := map[nebula.PartitionID]*storage.ScanCursor{pid: state.cursor}
        
        resp, err := s.client.ScanVertex(...)
        if err != nil {
            // 错误处理
            continue
        }
        
        // 处理数据
        vertexData := resp.GetProps()
        if vertexData != nil && len(vertexData.GetRows()) > 0 {
            hasData = true
            // 处理行...
        }
        
        // 更新分区状态
        cursors := resp.GetCursors()
        if cursor, ok := cursors[pid]; ok && len(cursor.GetNextCursor()) > 0 {
            state.cursor = cursor
        } else {
            state.done = true
        }
    }
    
    // 检查是否全部完成
    if !hasData || allDone(partStates) {
        break
    }
}
```

#### 2. ScanEdges

同样的改造逻辑应用于 ScanEdges 函数。

### 代码位置

| 函数 | 行号范围 |
|------|---------|
| ScanVertices | 267-416 |
| ScanEdges | 418-530 |

---

## 验证策略

### 测试场景
1. 单分区 space - 验证正常扫描
2. 多分区 space - 验证所有分区都被扫描
3. 部分分区无数据 - 验证跳过逻辑正确
4. 大数据量 - 验证 cursor 正确推进

### 日志建议
- 记录每个分区开始/完成
- 记录分区状态变化

---

## Commit Strategy

- **1**: `refactor(scanner): per-partition scan with independent state tracking`
  - Files: `service/copier/storage_scanner.go`
