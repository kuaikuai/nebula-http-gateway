package copier

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/dao"
)

const (
	maxRetries    = 6
	retryInterval = 5 * time.Second
)

const (
	maxVerifyRetries    = 10
	verifyRetrySleepSec = 3
)

var defaultBatchSize = beego.AppConfig.DefaultInt("copyBatchSize", 1000)

type contextKey string

const debugContextKey contextKey = "debug"

// debugLogCtx outputs the nGQL statement if debug mode is enabled (thread-safe)
func debugLogCtx(ctx context.Context, gql string) {
	if ctx.Value(debugContextKey).(bool) {
		logs.Info("[DEBUG nGQL] %s", gql)
	}
}

// executeWithGqlError wraps dao.Execute error with GQL statement for better debugging
func executeWithGqlError(nsid, gql string) error {
	_, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}
	return nil
}

// executeWithRetry 带重试的Execute，超时错误会重试
func executeWithRetry(nsid, gql string) (interface{}, interface{}, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		result, warn, err := dao.Execute(nsid, gql, nil)
		if err == nil {
			return result, warn, nil
		}

		// 仅超时错误重试或者属性元数据没有同步过来等待一下
		if !isRetryableError(err) {
			return result, warn, err
		}

		lastErr = err
		logs.Warn("[RETRY] Timeout error, retrying %d/%d: %s", i+1, maxRetries, gql)

		// 指数退避: 2s, 4s, 6s
		sleepDuration := retryInterval * time.Duration(i+1)
		time.Sleep(sleepDuration)
	}

	return nil, nil, fmt.Errorf("max retries (%d) exceeded for timeout: %w", maxRetries, lastErr)
}

// dropSpaceIfExists 检查并删除 space
func dropSpaceIfExists(nsid string, spaceName string) error {
	gql := "SHOW SPACES"
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}

	for _, row := range result.Tables {
		if name, ok := row["Name"].(string); ok && name == spaceName {
			gql := fmt.Sprintf("DROP SPACE %s", spaceName)
			if err := executeWithGqlError(nsid, gql); err != nil {
				return err
			}
			logs.Info("Dropped existing space: %s", spaceName)
			return nil
		}
	}
	return nil
}

// CopySpace copies all data from srcSpace to dstSpace
func CopySpace(ctx context.Context, nsid, srcSpace, dstSpace string, force bool, partitionNum, replicaFactor int, vidType string, debug bool, batchSize int) error {
	// 如果未指定 batchSize，使用配置文件中的默认值
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	ctx = context.WithValue(ctx, debugContextKey, debug)
	logs.Info("Starting to copy space from %s to %s (force=%v, partition_num=%v, replica_factor=%v, batch_size=%d)", srcSpace, dstSpace, force, partitionNum, replicaFactor, batchSize)
	// 如果 force 为 true，先检查并删除已存在的 space
	if force {
		if err := dropSpaceIfExists(nsid, dstSpace); err != nil {
			return fmt.Errorf("failed to drop existing space: %w", err)
		}
	}

	err := createSpace(ctx, nsid, srcSpace, dstSpace, partitionNum, replicaFactor, vidType)
	if err != nil {
		return fmt.Errorf("failed to create space: %v", err)
	}

	err = createIndexes(ctx, nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %v", err)
	}

	haveListeners, err := copyListeners(ctx, nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to copy listeners: %w", err)
	}
	if haveListeners {
		if err := copyFulltextIndexes(ctx, nsid, srcSpace, dstSpace); err != nil {
			return fmt.Errorf("failed to copy fulltext indexes: %w", err)
		}
	}

	tags, err := getTags(nsid, srcSpace)
	if err != nil {
		return fmt.Errorf("failed to get tags: %v", err)
	}

	edges, err := getEdges(nsid, srcSpace)
	if err != nil {
		return fmt.Errorf("failed to get edges: %v", err)
	}

	err = copyVertices(ctx, nsid, srcSpace, dstSpace, tags, batchSize)
	if err != nil {
		return fmt.Errorf("failed to copy vertices: %v", err)
	}

	err = copyEdges(ctx, nsid, srcSpace, dstSpace, edges, batchSize)
	if err != nil {
		return fmt.Errorf("failed to copy edges: %v", err)
	}

	// 执行负载均衡
	logs.Info("Submitting job to balance leader distribution...")
	balanceGql := fmt.Sprintf("USE %s; SUBMIT JOB BALANCE LEADER", dstSpace)
	debugLogCtx(ctx, balanceGql)
	_, _, err = dao.Execute(nsid, balanceGql, nil)
	if err != nil {
		logs.Warn("Balance leader job submission failed: %v", err)
		// 不阻塞复制流程，仅记录警告
	} else {
		logs.Info("Balance leader job submitted successfully")
	}

	return nil
}

// waitForSpaceReady waits for the space to be ready after creation
func waitForSpaceReady(nsid, spaceName string) error {
	maxRetries := 30
	// heartbeat_interval_secs TODO:
	retryInterval := 20 * time.Second

	for i := 0; i < maxRetries; i++ {
		time.Sleep(retryInterval)
		err := executeWithGqlError(nsid, fmt.Sprintf("USE %s", spaceName))
		if err == nil {
			return nil
		}
		logs.Warn("Space %s not ready yet, retrying %d/%d...: %v", spaceName, i+1, maxRetries, err)
	}
	return fmt.Errorf("space %s not ready after %d seconds", spaceName, maxRetries)
}

func createSpace(ctx context.Context, nsid, srcSpace, dstSpace string, partitionNum, replicaFactor int, vidType string) error {
	// 判断使用哪种流程
	if partitionNum == 0 && replicaFactor == 0 && vidType == "" {
		// 原流程: CREATE SPACE dstSpace AS srcSpace
		gql := fmt.Sprintf("CREATE SPACE %s AS %s", dstSpace, srcSpace)
		debugLogCtx(ctx, gql)
		err := executeWithGqlError(nsid, gql)
		if err != nil {
			return err
		}
		return waitForSpaceReady(nsid, dstSpace)
	}

	// 新流程: 获取源 space 元数据并构建完整 CREATE SPACE 语句
	desc, err := getSpaceDesc(nsid, srcSpace)
	if err != nil {
		return fmt.Errorf("failed to get space desc: %w", err)
	}
	// 构建 CREATE SPACE 语句
	gql := buildCreateSpaceGql(dstSpace, desc, partitionNum, replicaFactor, vidType)
	debugLogCtx(ctx, gql)
	if err := executeWithGqlError(nsid, gql); err != nil {
		return fmt.Errorf("failed to create space: %w", err)
	}
	err = waitForSpaceReady(nsid, dstSpace)
	if err != nil {
		return fmt.Errorf("space not ready: %w", err)
	}
	// 复制 tag schema
	if err := copyTagsAdvanced(ctx, nsid, srcSpace, dstSpace); err != nil {
		return fmt.Errorf("failed to copy tags: %w", err)
	}

	// 复制 edge schema
	if err := copyEdgesAdvanced(ctx, nsid, srcSpace, dstSpace); err != nil {
		return fmt.Errorf("failed to copy edges: %w", err)
	}

	// wait tag/edge schema to be ready
	return waitForEdgesAdvanced(ctx, nsid, srcSpace, dstSpace)
}

// SpaceDesc holds the metadata of a space from DESC SPACE
type SpaceDesc struct {
	ID            int64
	Name          string
	PartitionNum  int
	ReplicaFactor int
	Charset       string
	Collate       string
	VidType       string
	Comment       string
}

// getSpaceDesc 获取 space 的元数据
func getSpaceDesc(nsid, spaceName string) (*SpaceDesc, error) {
	gql := fmt.Sprintf("DESC SPACE %s", spaceName)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return nil, err
	}

	if len(result.Tables) == 0 {
		return nil, fmt.Errorf("no result from DESC SPACE %s", spaceName)
	}

	row := result.Tables[0]
	desc := &SpaceDesc{}

	if v, ok := row["ID"].(int64); ok {
		desc.ID = v
	}
	if v, ok := row["Name"].(string); ok {
		desc.Name = v
	}
	if v, ok := row["Partition Number"].(int64); ok {
		desc.PartitionNum = int(v)
	}
	if v, ok := row["Replica Factor"].(int64); ok {
		desc.ReplicaFactor = int(v)
	}
	if v, ok := row["Charset"].(string); ok {
		desc.Charset = v
	}
	if v, ok := row["Collate"].(string); ok {
		desc.Collate = v
	}
	if v, ok := row["Vid Type"].(string); ok {
		desc.VidType = v
	}
	if v, ok := row["Comment"].(string); ok {
		desc.Comment = v
	}

	return desc, nil
}

// buildCreateSpaceGql 构建完整的 CREATE SPACE 语句
func buildCreateSpaceGql(dstSpace string, desc *SpaceDesc, partitionNum, replicaFactor int, vidType string) string {
	// 使用源 space 的设置作为默认值
	pNum := desc.PartitionNum
	replica := desc.ReplicaFactor
	charset := desc.Charset
	collate := desc.Collate
	vType := desc.VidType
	comment := desc.Comment

	// 参数覆盖默认值
	if partitionNum > 0 {
		pNum = partitionNum
	}
	if replicaFactor > 0 {
		replica = replicaFactor
	}
	if vidType != "" {
		vType = vidType
	}

	return fmt.Sprintf("CREATE SPACE %s (partition_num = %d, replica_factor = %d, charset = %s, collate = %s, vid_type = %s) comment ='%s'",
		dstSpace, pNum, replica, charset, collate, vType, comment)
}

// copyTagsAdvanced 逐个复制 tag schema
func copyTagsAdvanced(ctx context.Context, nsid, srcSpace, dstSpace string) error {
	gql := fmt.Sprintf("USE %s; SHOW TAGS", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}

	for _, row := range result.Tables {
		if name, ok := row["Name"].(string); ok {
			createGql := fmt.Sprintf("USE %s; SHOW CREATE TAG %s", srcSpace, name)
			resp, _, err := dao.Execute(nsid, createGql, nil)
			if err != nil || len(resp.Tables) == 0 {
				continue
			}

			createStmt := ""
			for _, col := range resp.Headers {
				if strings.Contains(col, "Create Tag") {
					if stmt, ok := resp.Tables[0][col].(string); ok {
						createStmt = stmt
						break
					}
				}
			}
			if createStmt == "" {
				continue
			}

			execGql := fmt.Sprintf("USE %s; %s", dstSpace, createStmt)
			debugLogCtx(ctx, execGql)
			if err := executeWithGqlError(nsid, execGql); err != nil {
				return err
			}
		}
	}

	return nil
}

// copyEdgesAdvanced 逐个复制 edge schema
func copyEdgesAdvanced(ctx context.Context, nsid, srcSpace, dstSpace string) error {
	gql := fmt.Sprintf("USE %s; SHOW EDGES", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}

	for _, row := range result.Tables {
		if name, ok := row["Name"].(string); ok {
			createGql := fmt.Sprintf("USE %s; SHOW CREATE EDGE %s", srcSpace, name)
			resp, _, err := dao.Execute(nsid, createGql, nil)
			if err != nil || len(resp.Tables) == 0 {
				continue
			}

			createStmt := ""
			for _, col := range resp.Headers {
				if strings.Contains(col, "Create Edge") {
					if stmt, ok := resp.Tables[0][col].(string); ok {
						createStmt = stmt
						break
					}
				}
			}
			if createStmt == "" {
				continue
			}

			execGql := fmt.Sprintf("USE %s; %s", dstSpace, createStmt)
			debugLogCtx(ctx, execGql)
			if err := executeWithGqlError(nsid, execGql); err != nil {
				return err
			}
		}
	}

	return nil
}

func waitForEdgesAdvanced(ctx context.Context, nsid, srcSpace, dstSpace string) error {
	//TODO better method
	time.Sleep(20 * time.Second)
	return nil
}

func waitForTagsAdvanced(ctx context.Context, nsid, srcSpace, dstSpace string) error {
	//TODO
	return nil
}

func getTags(nsid, space string) ([]string, error) {
	gql := fmt.Sprintf("USE %s; SHOW TAGS", space)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return nil, fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}
	var tags []string
	for _, row := range result.Tables {
		if name, ok := row["Name"].(string); ok {
			tags = append(tags, name)
		}
	}
	return tags, nil
}

func getEdges(nsid, space string) ([]string, error) {
	gql := fmt.Sprintf("USE %s; SHOW EDGES", space)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return nil, fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}
	var edges []string
	for _, row := range result.Tables {
		if name, ok := row["Name"].(string); ok {
			edges = append(edges, name)
		}
	}
	return edges, nil
}

func getSpaceID(nsid, spaceName string) (int64, error) {
	gql := fmt.Sprintf("DESCRIBE SPACE %s", spaceName)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return 0, fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}

	for _, row := range result.Tables {
		if id, ok := row["ID"].(int64); ok {
			return id, nil
		}
	}
	return 0, fmt.Errorf("space ID not found for space %s", spaceName)
}

// copyFulltextIndexes 复制全文本索引到新 space
func copyFulltextIndexes(ctx context.Context, nsid, srcSpace, dstSpace string) error {
	// 1. 获取 dstSpace 的 ID
	dstSpaceID, err := getSpaceID(nsid, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to get dst space ID: %w", err)
	}

	// 2. 在原 space 获取全文本索引信息
	gql := fmt.Sprintf("USE %s; SHOW FULLTEXT INDEXES", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}

	if len(result.Tables) == 0 {
		logs.Info("[DEBUG] No fulltext indexes found in source space")
		return nil
	}

	// 3. 解析并创建全文本索引
	for _, row := range result.Tables {
		indexName, _ := row["Name"].(string)
		schemaType, _ := row["Schema Type"].(string) // Tag 或 Edge
		schemaName, _ := row["Schema Name"].(string)
		fields, _ := row["Fields"].(string)
		analyzer, _ := row["Analyzer"].(string)

		if indexName == "" || schemaName == "" || fields == "" {
			continue
		}

		// 生成新索引名：只有 text_idx_xxx 或 knn_idx_xxx 格式才替换 space_name；否则加上 space ID
		var newIndexName string
		lowerIndexName := strings.ToLower(indexName)
		lowerSrcSpace := strings.ToLower(srcSpace)
		if (strings.HasPrefix(lowerIndexName, "text_idx_") || strings.HasPrefix(lowerIndexName, "knn_idx_")) &&
			strings.Contains(lowerIndexName, lowerSrcSpace) {
			// 替换 space_name 部分
			newIndexName = strings.Replace(indexName, srcSpace, dstSpace, 1)
		} else {
			// 原规则：在索引名后加上 space ID 避免冲突
			newIndexName = fmt.Sprintf("%s_%d", indexName, dstSpaceID)
		}

		// 4. 生成 CREATE FULLTEXT INDEX nGQL
		var createGql string
		if schemaType == "Tag" {
			createGql = fmt.Sprintf("CREATE FULLTEXT TAG INDEX %s ON %s(%s) ANALYZER=\"%s\"",
				newIndexName, schemaName, fields, analyzer)
		} else if schemaType == "Edge" {
			createGql = fmt.Sprintf("CREATE FULLTEXT EDGE INDEX %s ON %s(%s) ANALYZER=\"%s\"",
				newIndexName, schemaName, fields, analyzer)
		} else {
			continue
		}

		fullGql := fmt.Sprintf("USE %s; %s", dstSpace, createGql)
		debugLogCtx(ctx, fullGql)

		logs.Info("[DEBUG] Creating fulltext index: %s (original: %s)", newIndexName, indexName)

		if err := executeWithGqlError(nsid, fullGql); err != nil {
			return fmt.Errorf("create fulltext index %s failed: %w", newIndexName, err)
		}
	}

	logs.Info("[DEBUG] Fulltext indexes copied successfully")
	return nil
}

// copyListeners 复制 LISTENER 配置到新 space
func copyListeners(ctx context.Context, nsid, srcSpace, dstSpace string) (bool, error) {
	// 1. 在原 space 获取 LISTENER 信息
	gql := fmt.Sprintf("USE %s; SHOW LISTENER", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return false, fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}

	if len(result.Tables) == 0 {
		logs.Info("[DEBUG] No listeners found in source space")
		return false, nil
	}

	// 2. 提取 ELASTICSEARCH 类型的 Host
	var hosts []string
	for _, row := range result.Tables {
		if listenerType, ok := row["Type"].(string); ok && listenerType == "ELASTICSEARCH" {
			if host, ok := row["Host"].(string); ok {
				hosts = append(hosts, host)
			}
		}
	}

	if len(hosts) == 0 {
		logs.Info("No ELASTICSEARCH listeners found")
		return false, nil
	}

	// 3. 生成 ADD LISTENER nGQL 并执行
	hostsStr := strings.Join(hosts, ",")
	addListenerGql := fmt.Sprintf("ADD LISTENER ELASTICSEARCH %s", hostsStr)
	fullGql := fmt.Sprintf("USE %s; %s", dstSpace, addListenerGql)

	logs.Info("Copying listener: %s", addListenerGql)
	debugLogCtx(ctx, fullGql)

	_, _, err = dao.Execute(nsid, fullGql, nil)
	if err != nil {
		return false, fmt.Errorf("add listener failed: %w", err)
	}

	logs.Info("Listener copied successfully")
	return true, nil
}

// verifyIndex verifies that an index was created successfully by running REBUILD command.
// If SemanticError: Index is returned, it retries after sleeping (for async metadata sync).
func verifyIndex(nsid, spaceName, indexType, indexName string) error {
	gql := fmt.Sprintf("USE %s; REBUILD %s INDEX %s", spaceName, indexType, indexName)
	var lastErr error
	for i := 0; i < maxVerifyRetries; i++ {
		_, _, err := dao.Execute(nsid, gql, nil)
		if err == nil {
			return nil
		}
		// Check if it's a SemanticError related to index not ready yet
		if strings.Contains(err.Error(), "SemanticError") && strings.Contains(err.Error(), "Index") {
			lastErr = err
			logs.Warn("[VERIFY RETRY] SemanticError for index %s, retrying %d/%d after %d seconds",
				indexName, i+1, maxVerifyRetries, verifyRetrySleepSec)
			time.Sleep(time.Duration(verifyRetrySleepSec) * time.Second)
			continue
		}
		// Other errors - return immediately
		return fmt.Errorf("verify index %s failed: %w", indexName, err)
	}
	return fmt.Errorf("verify index %s failed after %d retries: %w", indexName, maxVerifyRetries, lastErr)
}

func createIndexes(ctx context.Context, nsid, srcSpace, dstSpace string) error {
	// Phase 1: Get all tag index names
	tagIndexResult, _, _ := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW TAG INDEXES", srcSpace), nil)
	var tagIndexNames []string
	for _, row := range tagIndexResult.Tables {
		if indexName, ok := row["Index Name"].(string); ok {
			tagIndexNames = append(tagIndexNames, indexName)
		}
	}

	// Phase 2: Get all edge index names
	edgeIndexResult, _, _ := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW EDGE INDEXES", srcSpace), nil)
	var edgeIndexNames []string
	for _, row := range edgeIndexResult.Tables {
		if indexName, ok := row["Index Name"].(string); ok {
			edgeIndexNames = append(edgeIndexNames, indexName)
		}
	}

	// Phase 3: Batch create all Tag indexes
	for _, indexName := range tagIndexNames {
		if err := createTagIndex(ctx, nsid, srcSpace, dstSpace, indexName); err != nil {
			if !strings.Contains(err.Error(), "Existed") {
				return fmt.Errorf("create tag index %s failed: %w", indexName, err)
			}
		}
	}

	// Phase 4: Batch create all Edge indexes
	for _, indexName := range edgeIndexNames {
		if err := createEdgeIndex(ctx, nsid, srcSpace, dstSpace, indexName); err != nil {
			if !strings.Contains(err.Error(), "Existed") {
				return fmt.Errorf("create edge index %s failed: %w", indexName, err)
			}
		}
	}

	// Phase 5: Unified verify all Tag indexes
	for _, indexName := range tagIndexNames {
		if err := verifyIndex(nsid, dstSpace, "TAG", indexName); err != nil {
			return fmt.Errorf("verify tag index %s failed: %w", indexName, err)
		}
	}

	// Phase 6: Unified verify all Edge indexes
	for _, indexName := range edgeIndexNames {
		if err := verifyIndex(nsid, dstSpace, "EDGE", indexName); err != nil {
			return fmt.Errorf("verify edge index %s failed: %w", indexName, err)
		}
	}

	return nil
}

func createTagIndex(ctx context.Context, nsid, srcSpace, dstSpace, indexName string) error {
	gql := fmt.Sprintf("USE %s; SHOW CREATE TAG INDEX %s", srcSpace, indexName)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil || len(result.Tables) == 0 {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}
	row := result.Tables[0]
	var createStmt string
	for _, col := range result.Headers {
		if strings.Contains(col, "Create") {
			if stmt, ok := row[col].(string); ok {
				createStmt = stmt
				break
			}
		}
	}
	if createStmt == "" {
		return fmt.Errorf("cannot find create statement for index %s", indexName)
	}
	execGql := fmt.Sprintf("USE %s; %s", dstSpace, createStmt)
	debugLogCtx(ctx, execGql)
	return executeWithGqlError(nsid, execGql)
}

func createEdgeIndex(ctx context.Context, nsid, srcSpace, dstSpace, indexName string) error {
	gql := fmt.Sprintf("USE %s; SHOW CREATE EDGE INDEX %s", srcSpace, indexName)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil || len(result.Tables) == 0 {
		return fmt.Errorf("execute GQL failed, gql: %s, error: %w", gql, err)
	}
	row := result.Tables[0]
	var createStmt string
	for _, col := range result.Headers {
		if strings.Contains(col, "Create") {
			if stmt, ok := row[col].(string); ok {
				createStmt = stmt
				break
			}
		}
	}
	if createStmt == "" {
		return fmt.Errorf("cannot find create statement for index %s", indexName)
	}
	execGql := fmt.Sprintf("USE %s; %s", dstSpace, createStmt)
	debugLogCtx(ctx, execGql)
	return executeWithGqlError(nsid, execGql)
}

func copyVertices(ctx context.Context, nsid, srcSpace, dstSpace string, tags []string, batchSize int) error {
	for _, tag := range tags {
		logs.Info("Copying vertices for tag: %s", tag)
		scanner, err := NewStorageScanner(nsid, srcSpace)
		if err != nil {
			return err
		}
		defer scanner.Close()

		// 扫描源 space 的 vertex
		err = scanner.ScanVertices(tag, batchSize, func(vertices []map[string]interface{}) error {
			logs.Info("Scanned %d vertices for tag %s", len(vertices), tag)
			// 批量插入到目标 space
			if err := insertVertexBatch(ctx, nsid, dstSpace, tag, vertices); err != nil {
				logs.Error("Insert vertex failed: %v", err)
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func insertVertexBatch(ctx context.Context, nsid, dstSpace, tag string, vertices []map[string]interface{}) error {
	if len(vertices) == 0 {
		return nil
	}

	// 构建 INSERT VERTEX 语句
	// INSERT VERTEX tag_name() VALUES vid: (props)
	// 或 INSERT VERTEX tag_name(prop1, prop2) VALUES vid: (values)

	// 收集属性名
	props := make([]string, 0)
	for _, v := range vertices {
		for k := range v {
			if k != "vid" && k != "_vid" {
				props = append(props, k)
			}
		}
		break
	}

	// 构建批量插入语句
	values := make([]string, 0, len(vertices))
	for _, v := range vertices {
		vid := v["vid"]
		if vid == nil {
			continue
		}

		vidStr := formatVid(vid)

		if len(props) == 0 {
			values = append(values, fmt.Sprintf("%s: ()", vidStr))
		} else {
			propVals := make([]string, 0, len(props))
			for _, p := range props {
				if val, ok := v[p]; ok {
					propVals = append(propVals, formatValue(val))
				} else {
					propVals = append(propVals, "NULL")
				}
			}
			values = append(values, fmt.Sprintf("%s: (%s)", vidStr, strings.Join(propVals, ", ")))
		}
	}

	if len(values) == 0 {
		return nil
	}

	var insertGql string
	if len(props) == 0 {
		insertGql = fmt.Sprintf("USE %s; INSERT VERTEX %s VALUES %s", dstSpace, tag, strings.Join(values, ", "))
	} else {
		insertGql = fmt.Sprintf("USE %s; INSERT VERTEX %s(%s) VALUES %s", dstSpace, tag, strings.Join(props, ", "), strings.Join(values, ", "))
	}

	debugLogCtx(ctx, insertGql)
	_, _, err := executeWithRetry(nsid, insertGql)
	if err != nil {
		logs.Error("Insert vertex failed: %v", err)
		return err
	}

	return nil
}

// formatVid formats the vertex/edge ID for use in INSERT statements
func formatVid(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", escapeString(val))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("\"%s\"", escapeString(fmt.Sprintf("%v", val)))
	}
}

// formatValue formats a property value for use in INSERT statements
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", escapeString(val))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%f", val)
	case float64:
		return fmt.Sprintf("%f", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return "null"
	}
}

// escapeString escapes special characters in a string for Nebula Graph INSERT statements
func escapeString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return s
}

func copyEdges(ctx context.Context, nsid, srcSpace, dstSpace string, edges []string, batchSize int) error {

	for _, edge := range edges {
		logs.Info("Copying edges for type: %s", edge)
		scanner, err := NewStorageScanner(nsid, srcSpace)
		if err != nil {
			return err
		}
		defer scanner.Close()

		// 扫描源 space 的 edge
		err = scanner.ScanEdges(edge, batchSize, func(edges []map[string]interface{}) error {
			logs.Info("Scanned %d edges for type %s", len(edges), edge)
			// 批量插入到目标 space
			if err := insertEdgeBatch(ctx, nsid, dstSpace, edge, edges); err != nil {
				logs.Error("Insert edge failed: %v", err)
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func insertEdgeBatch(ctx context.Context, nsid, dstSpace, edge string, edges []map[string]interface{}) error {
	if len(edges) == 0 {
		return nil
	}

	// 收集属性名
	props := make([]string, 0)
	for _, e := range edges {
		for k := range e {
			if k != "_src" && k != "_dst" && k != "_rank" {
				props = append(props, k)
			}
		}
		break
	}

	// 构建批量插入语句
	// INSERT EDGE edge_name() VALUES src -> dst (rank): ()
	// 或 INSERT EDGE edge_name(prop1, prop2) VALUES src -> dst (rank): (values)
	values := make([]string, 0, len(edges))
	for _, e := range edges {
		src := e["_src"]
		dst := e["_dst"]
		rank := e["_rank"]
		if src == nil || dst == nil {
			continue
		}

		srcStr := formatVid(src)
		dstStr := formatVid(dst)

		// 构建边值: rank=0 不显示@rank, rank>0 显示@rank
		var edgeValue string
		hasProps := len(props) > 0
		hasRank := false
		if rank != nil {
			if r, ok := rank.(int64); ok && r != 0 {
				hasRank = true
			}
		}

		if !hasProps && !hasRank {
			edgeValue = fmt.Sprintf("%s -> %s: ()", srcStr, dstStr)
		} else if !hasProps && hasRank {
			rankStr := formatValue(rank)
			edgeValue = fmt.Sprintf("%s -> %s@%s: ()", srcStr, dstStr, rankStr)
		} else if hasProps && !hasRank {
			propVals := make([]string, 0, len(props))
			for _, p := range props {
				if val, ok := e[p]; ok {
					propVals = append(propVals, formatValue(val))
				} else {
					propVals = append(propVals, "NULL")
				}
			}
			edgeValue = fmt.Sprintf("%s -> %s: (%s)", srcStr, dstStr, strings.Join(propVals, ", "))
		} else {
			rankStr := formatValue(rank)
			propVals := make([]string, 0, len(props))
			for _, p := range props {
				if val, ok := e[p]; ok {
					propVals = append(propVals, formatValue(val))
				} else {
					propVals = append(propVals, "NULL")
				}
			}
			edgeValue = fmt.Sprintf("%s -> %s@%s: (%s)", srcStr, dstStr, rankStr, strings.Join(propVals, ", "))
		}

		values = append(values, edgeValue)
	}

	if len(values) == 0 {
		return nil
	}

	var insertGql string
	if len(props) == 0 {
		insertGql = fmt.Sprintf("USE %s; INSERT EDGE %s VALUES %s", dstSpace, edge, strings.Join(values, ", "))
	} else {
		insertGql = fmt.Sprintf("USE %s; INSERT EDGE %s(%s) VALUES %s", dstSpace, edge, strings.Join(props, ", "), strings.Join(values, ", "))
	}

	debugLogCtx(ctx, insertGql)
	_, _, err := executeWithRetry(nsid, insertGql)
	if err != nil {
		logs.Error("Insert edge failed: %v", err)
		return err
	}

	return nil
}
