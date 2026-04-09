# createIndexes 索引验证重试机制

## TL;DR

> **Quick Summary**: 为 `createIndexes` 函数添加批量创建 + 统一验证逻辑，使用 `REBUILD INDEX` 验证，遇到 `SemanticError: Index` 时自动重试
> 
> **Deliverables**:
> - 修改 `createIndexes` 实现**批量创建 + 统一验证**流程
> - 新增 `verifyIndex` 函数 - 单个索引验证 + 重试
> - 修改 `createTagIndex` - 仅创建，不验证
> - 修改 `createEdgeIndex` - 仅创建，不验证
> 
> **Estimated Effort**: Small (< 1 hour)
> **Parallel Execution**: NO - sequential changes in single file
> **Critical Path**: copier.go:549 → copier.go:612

---

## Context

### Original Request
用户要求在 `createIndexes` 创建索引后，使用 `REBUILD TAG INDEX index_name` 或 `REBUILD EDGE INDEX index_name` 验证索引是否创建成功。如果报错 `SemanticError: Index`，需要休眠几秒后重试，保证异步元数据成功。

**用户偏好**: 先批量创建所有索引，**最后统一验证**，这样效率更高。

### Current Code Analysis
- **位置**: `service/copier/copier.go`
- **问题**: 
  1. `createIndexes` (line 549-564) 调用 `createTagIndex` 和 `createEdgeIndex` 时**忽略返回值**
  2. 创建索引后没有验证步骤
  3. NebulaGraph 异步元数据同步可能导致立即查询失败

### Code Flow (Current)
```
createIndexes (549)
  ├── createTagIndex (566) ← ⚠️ 无验证，无重试
  └── createEdgeIndex (590) ← ⚠️ 无验证，无重试
```

### Code Flow (New - 批量创建 + 统一验证)
```
createIndexes (549)
  │
  ├── Phase 1: 创建所有 Tag 索引
  │   └── 收集 tag index names
  │
  ├── Phase 2: 创建所有 Edge 索引
  │   └── 收集 edge index names
  │
  ├── Phase 3: 统一验证所有 Tag 索引
  │   └── for each tag_index: verifyIndex(TAG)
  │
  └── Phase 4: 统一验证所有 Edge 索引
      └── for each edge_index: verifyIndex(EDGE)
```

---

## Work Objectives

### Core Objective
为索引创建添加**批量创建 + 统一验证**机制，使用 `REBUILD INDEX` 验证 + `SemanticError` 重试

### Concrete Deliverables
- [ ] `verifyIndex` 函数: 单个索引验证 + SemanticError 重试
- [ ] `createTagIndex`: 移除验证逻辑（仅创建）
- [ ] `createEdgeIndex`: 移除验证逻辑（仅创建）
- [ ] `createIndexes`: 批量创建 + 统一验证流程

### Definition of Done
- [ ] 先批量创建所有索引，再统一验证
- [ ] `REBUILD INDEX index_name` 验证
- [ ] 遇到 `SemanticError: Index` 时休眠 3 秒后重试
- [ ] 最多重试 3 次
- [ ] 所有错误正确传播给调用者

### Must Have
- 索引创建和验证分离（先创建所有，再验证所有）
- SemanticError 必须触发重试机制
- 错误必须传播到调用者

### Must NOT Have
- 不修改无关文件
- 不在 createTagIndex/createEdgeIndex 中调用 verifyIndex（验证在 createIndexes 统一做）

---

## Verification Strategy

### Test Decision
- **Infrastructure exists**: NO (Go 项目无测试框架)
- **Automated tests**: None
- **Agent-Executed QA**: Yes - 手动验证修改正确性

---

## Execution Strategy

### Task Structure
单一文件修改，顺序执行：

1. **新增 verifyIndex 函数** - 验证 + 重试逻辑（基础函数）
2. **修改 createTagIndex** - 移除验证，仅创建
3. **修改 createEdgeIndex** - 移除验证，仅创建
4. **修改 createIndexes** - 批量创建 + 统一验证

### Dependency
所有任务在同一个文件中，verifyIndex 必须先定义（Task 1），然后其他任务才能使用它。

---

## TODOs

- [ ] 1. 新增 verifyIndex 函数 - 验证索引 + SemanticError 重试

  **What to do**:
  - 在 `createIndexes` 函数附近新增 `verifyIndex` 函数
  - 签名: `func verifyIndex(nsid, spaceName, indexType, indexName string) error`
  - 执行 `REBUILD {indexType} INDEX {indexName}` 验证
  - 如果返回 SemanticError，休眠 3 秒后重试，最多 3 次
  - 定义常量: `maxVerifyRetries = 3`, `verifyRetrySleep = 3 * time.Second`
  - 验证成功返回 nil，验证失败返回 error

  **Must NOT do**:
  - 不修改已有函数签名
  - 不在其他文件中添加代码

  **Recommended Agent Profile**:
  > - **Category**: `quick` - 单一文件小改动
  > - **Skills**: []

  **Pattern Reference**:
  ```go
  // 参考 executeWithRetry 的重试模式 (lines 42-64)
  for i := 0; i < maxRetries; i++ {
      result, warn, err := dao.Execute(nsid, gql, nil)
      if err == nil {
          return nil
      }
      if !isRetryableError(err) {
          return err
      }
      time.Sleep(sleepDuration)
  }
  ```

  **Acceptance Criteria**:
  - [ ] 函数签名: `func verifyIndex(nsid, spaceName, indexType, indexName string) error`
  - [ ] 执行 `REBUILD {indexType} INDEX {indexName}` 在正确的 space
  - [ ] SemanticError 触发重试逻辑
  - [ ] 最多重试 3 次
  - [ ] 非 SemanticError 错误立即返回

  **QA Scenarios**:
  ```
  Scenario: 验证成功 - 索引已就绪
    Preconditions: 索引创建成功，REBUILD 立即返回成功
    Steps:
      1. 执行 REBUILD TAG INDEX index_name
      2. 无错误返回
    Expected Result: verifyIndex 返回 nil
    Evidence: 代码逻辑审查

  Scenario: 验证失败 - SemanticError Index (异步同步中)
    Preconditions: 索引创建成功，但元数据尚未同步，REBUILD 返回 SemanticError: Index
    Steps:
      1. 执行 REBUILD，返回 SemanticError
      2. 休眠 3 秒
      3. 重试 REBUILD
      4. 重复直到成功或达到最大重试次数
    Expected Result: 最终成功或返回错误
    Evidence: 代码逻辑审查

  Scenario: 验证失败 - 非 SemanticError 错误
    Preconditions: REBUILD 返回其他错误（如网络错误）
    Steps:
      1. 执行 REBUILD，返回网络错误
      2. 不重试，立即返回错误
    Expected Result: verifyIndex 返回该错误
    Evidence: 代码逻辑审查
  ```

  **Commit**: YES
  - Message: `fix(copier): add verifyIndex function with retry logic`
  - Files: `service/copier/copier.go`

---

- [ ] 2. 修改 createTagIndex - 移除验证逻辑，仅创建

  **What to do**:
  - `createTagIndex` 保持原有创建逻辑不变
  - **不需要在创建后调用 verifyIndex**（验证在 createIndexes 统一做）
  - 确保函数签名不变: `func createTagIndex(ctx context.Context, nsid, srcSpace, dstSpace, indexName string) error`

  **Must NOT do**:
  - 不添加 verifyIndex 调用
  - 不修改创建逻辑

  **Recommended Agent Profile**:
  > - **Category**: `quick`
  > - **Skills**: []

  **Acceptance Criteria**:
  - [ ] createTagIndex 保持原有逻辑
  - [ ] 不调用 verifyIndex

  **Commit**: YES (grouped with task 1, 3, 4)
  - Message: `fix(copier): separate index creation from verification`
  - Files: `service/copier/copier.go`

---

- [ ] 3. 修改 createEdgeIndex - 移除验证逻辑，仅创建

  **What to do**:
  - `createEdgeIndex` 保持原有创建逻辑不变
  - **不需要在创建后调用 verifyIndex**（验证在 createIndexes 统一做）
  - 确保函数签名不变: `func createEdgeIndex(ctx context.Context, nsid, srcSpace, dstSpace, indexName string) error`

  **Must NOT do**:
  - 不添加 verifyIndex 调用
  - 不修改创建逻辑

  **Recommended Agent Profile**:
  > - **Category**: `quick`
  > - **Skills**: []

  **Acceptance Criteria**:
  - [ ] createEdgeIndex 保持原有逻辑
  - [ ] 不调用 verifyIndex

  **Commit**: YES (grouped with task 1, 2, 4)
  - Message: `fix(copier): separate index creation from verification`
  - Files: `service/copier/copier.go`

---

- [ ] 4. 修改 createIndexes - 批量创建 + 统一验证

  **What to do**:
  重构 `createIndexes` 实现**批量创建 + 统一验证**流程：

  ```go
  func createIndexes(ctx context.Context, nsid, srcSpace, dstSpace string) error {
      // Phase 1: 获取所有 tag index names
      tagIndexResult, _, _ := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW TAG INDEXES", srcSpace), nil)
      var tagIndexNames []string
      for _, row := range tagIndexResult.Tables {
          if indexName, ok := row["Index Name"].(string); ok {
              tagIndexNames = append(tagIndexNames, indexName)
          }
      }

      // Phase 2: 获取所有 edge index names
      edgeIndexResult, _, _ := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW EDGE INDEXES", srcSpace), nil)
      var edgeIndexNames []string
      for _, row := range edgeIndexResult.Tables {
          if indexName, ok := row["Index Name"].(string); ok {
              edgeIndexNames = append(edgeIndexNames, indexName)
          }
      }

      // Phase 3: 批量创建所有 Tag 索引
      for _, indexName := range tagIndexNames {
          if err := createTagIndex(ctx, nsid, srcSpace, dstSpace, indexName); err != nil {
              return err
          }
      }

      // Phase 4: 批量创建所有 Edge 索引
      for _, indexName := range edgeIndexNames {
          if err := createEdgeIndex(ctx, nsid, srcSpace, dstSpace, indexName); err != nil {
              return err
          }
      }

      // Phase 5: 统一验证所有 Tag 索引
      for _, indexName := range tagIndexNames {
          if err := verifyIndex(nsid, dstSpace, "TAG", indexName); err != nil {
              return fmt.Errorf("verify tag index %s failed: %w", indexName, err)
          }
      }

      // Phase 6: 统一验证所有 Edge 索引
      for _, indexName := range edgeIndexNames {
          if err := verifyIndex(nsid, dstSpace, "EDGE", indexName); err != nil {
              return fmt.Errorf("verify edge index %s failed: %w", indexName, err)
          }
      }

      return nil
  }
  ```

  **Must NOT do**:
  - 不在创建循环中调用验证
  - 不改变 createTagIndex 和 createEdgeIndex 的逻辑

  **Recommended Agent Profile**:
  > - **Category**: `quick`
  > - **Skills**: []

  **Acceptance Criteria**:
  - [ ] 先收集所有 index names
  - [ ] 先批量创建所有 tag 索引
  - [ ] 先批量创建所有 edge 索引
  - [ ] 再统一验证所有 tag 索引（调用 verifyIndex）
  - [ ] 再统一验证所有 edge 索引（调用 verifyIndex）
  - [ ] 错误正确传播

  **QA Scenarios**:
  ```
  Scenario: 所有索引创建并验证成功
    Preconditions: srcSpace 有 2 个 tag index，1 个 edge index
    Steps:
      1. 创建所有 tag 索引
      2. 创建所有 edge 索引
      3. 验证所有 tag 索引 (REBUILD)
      4. 验证所有 edge 索引 (REBUILD)
    Expected Result: createIndexes 返回 nil

  Scenario: 创建索引失败
    Preconditions: 某个 tag 索引创建失败
    Steps:
      1. 创建所有 tag 索引
      2. createTagIndex 返回 error
      3. 立即返回错误，不继续创建其他索引
    Expected Result: createIndexes 返回该错误

  Scenario: 验证索引遇到 SemanticError
    Preconditions: tag 索引创建成功，但验证时遇到 SemanticError
    Steps:
      1. 创建所有 tag 索引
      2. 创建所有 edge 索引
      3. 验证 tag 索引
      4. REBUILD 返回 SemanticError: Index
      5. 休眠 3 秒
      6. 重试 REBUILD
      7. 成功则继续，失败则返回错误
    Expected Result: 最多重试 3 次后成功或返回错误

  Scenario: 验证索引遇到非 SemanticError 错误
    Preconditions: REBUILD 返回网络错误
    Steps:
      1. 创建所有索引
      2. 验证 tag 索引
      3. REBUILD 返回网络错误
      4. 不重试，立即返回错误
    Expected Result: createIndexes 返回该错误
  ```

  **Commit**: YES (grouped with task 1, 2, 3)
  - Message: `fix(copier): batch create indexes then unified verification`
  - Files: `service/copier/copier.go`

---

## Final Verification Wave

- [ ] F1. **Plan Compliance Audit** — `oracle`
  验证所有 Must Have 已实现:
  - [ ] verifyIndex 函数存在且逻辑正确
  - [ ] createTagIndex 不调用 verifyIndex（仅创建）
  - [ ] createEdgeIndex 不调用 verifyIndex（仅创建）
  - [ ] createIndexes 先批量创建，再统一验证
  - [ ] SemanticError 触发重试

- [ ] F2. **Code Quality Review** — `unspecified-high`
  - 代码可以编译
  - 无明显语法错误
  - 遵循现有代码风格

- [ ] F3. **Scope Fidelity Check** — `deep`
  - 仅修改 `service/copier/copier.go`
  - 无其他文件被修改

---

## Success Criteria

### Final Checklist
- [ ] 所有 Must Have 满足
- [ ] 所有 Must NOT Have 未被违反
- [ ] 代码可以编译
- [ ] 逻辑符合用户需求（批量创建 + 统一验证）
