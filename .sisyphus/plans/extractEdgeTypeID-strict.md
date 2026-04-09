# extractEdgeTypeID 函数修改计划

## TL;DR

> 修改 `extractEdgeTypeID` 函数，使其更加严格地从 `edgeProps` 数组中提取**正数**的 type 值，避免错误匹配到顶层的 `"type": "DATASET"` 或负数 type（如 -31）。

## Context

### 问题描述
用户提供的 JSON 数据：
```json
"edgeProps": [
  {
    "type": -31,
    "props": ["_src", "_type", "_rank", "_dst"]
  },
  {
    "type": 31,
    "props": ["_src", "_type", "_rank", "_dst"]
  }
]
```

当前函数错误地将 `-31` 解释为 `26`（可能是匹配到了其他位置的 "type"）。

### 当前实现问题
1. **匹配过于宽泛**：使用简单字符串搜索 `"type"`，会匹配到 JSON 中所有包含 "type" 的位置
2. **负数处理错误**：遇到 `-31` 时会跳过负号，提取出错误的值
3. **没有边界限制**：没有限定只在 `edgeProps` 范围内查找

---

## Work Objectives

### Core Objective
修改 `extractEdgeTypeID` 函数，使其：
1. **只在 `edgeProps` 数组范围内查找** type 值
2. **忽略负数 type**（只返回正数）
3. **严格匹配** `"type"` 作为完整的 JSON key

### Concrete Deliverables
- 修改文件：`service/copier/storage_scanner.go`
- 修改函数：`extractEdgeTypeID` (第 215-237 行)

### Definition of Done
- [x] 函数只从 `edgeProps` 数组中提取 type
- [x] 函数只返回正数 type，负数被忽略
- [x] 不会错误匹配到顶层的 `"type": "DATASET"`

---

## Verification Strategy

### Test Strategy
- **Infrastructure exists**: 项目使用 Go，无需额外测试框架
- **Automated tests**: 无
- **Agent-Executed QA**: 手动验证

### QA Scenarios

**Scenario 1: 正数 type 提取**
- Tool: 直接代码审查
- Input: edgeProps 中包含 `"type": 31`
- Expected: 返回 31, true

**Scenario 2: 负数 type 忽略**
- Tool: 直接代码审查
- Input: edgeProps 中包含 `"type": -31` 和 `"type": 31`
- Expected: 返回 31, true（忽略 -31）

**Scenario 3: 顶层 type 不被匹配**
- Tool: 直接代码审查
- Input: 包含顶层 `"type": "DATASET"` 和 edgeProps 中的 type
- Expected: 只返回 edgeProps 中的 type

---

## Execution Strategy

### Task Breakdown

仅一个任务，无需分波：

- [x] 1. 修改 `extractEdgeTypeID` 函数

  **What to do**:
  - 读取当前函数实现
  - 替换为更严格的实现逻辑：
    1. 先找到 `edgeProps` 关键字位置
    2. 找到 `[` 和 `]` 界定数组范围
    3. 在数组范围内查找 `"type"` 作为完整 key
    4. 检查负号，如有负号则跳过该值
    5. 只返回正数

  **Must NOT do**:
  - 不修改函数签名
  - 不修改调用处的代码

  **References**:
  - `service/copier/storage_scanner.go:215-237` - 当前实现

  **Acceptance Criteria**:
  - [x] 代码编译通过 (`go build`)
  - [x] 函数逻辑符合要求

---

## Commit Strategy

- **1**: `fix(scanner): strict edge type extraction in extractEdgeTypeID`
  - Files: `service/copier/storage_scanner.go`
  - Pre-commit: `go build ./...`

---

## Success Criteria

### Verification Commands
```bash
go build ./service/copier/
```

### Final Checklist
- [x] 函数只从 edgeProps 中提取 type
- [x] 负数 type 被忽略
- [x] 顶层 type 不被误匹配
- [x] 代码编译通过
