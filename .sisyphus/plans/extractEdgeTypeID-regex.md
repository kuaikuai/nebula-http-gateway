# extractEdgeTypeID 使用正则表达式

## TL;DR

> 将 `extractEdgeTypeID` 函数改为使用正则表达式匹配 `"type": 34` 格式，只提取正数。

## Work Objectives

### Core Objective
使用正则表达式匹配 `edgeProps` 数组中的 type 值

### Concrete Deliverables
- 修改文件：`service/copier/storage_scanner.go`
- 添加 `regexp` import
- 重写 `extractEdgeTypeID` 函数使用正则

### 实现逻辑
1. 找到 `edgeProps` 数组边界
2. 使用正则 `"type":\s*(\d+)` 匹配正数 type
3. 返回第一个匹配的正数

### Definition of Done
- [x] 使用正则表达式 `"type":\s*(\d+)"` 匹配
- [x] 只返回正数，负数忽略
- [x] 代码编译通过
- [ ] 使用正则表达式 `"type":\s*(\d+)"` 匹配
- [ ] 只返回正数，负数忽略
- [ ] 代码编译通过
