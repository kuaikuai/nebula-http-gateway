package copier

import (
	"fmt"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/dao"
)

const (
	maxRetries    = 6
	retryInterval = 5 * time.Second
)

// executeWithRetry 带重试的Execute，超时错误会重试
func executeWithRetry(nsid, gql string) (interface{}, interface{}, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		result, warn, err := dao.Execute(nsid, gql, nil)
		if err == nil {
			return result, warn, nil
		}

		// 仅超时错误重试
		if !isTimeoutError(err) {
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
	result, _, err := dao.Execute(nsid, "SHOW SPACES", nil)
	if err != nil {
		return err
	}

	for _, row := range result.Tables {
		if name, ok := row["Name"].(string); ok && name == spaceName {
			_, _, err := dao.Execute(nsid, fmt.Sprintf("DROP SPACE %s", spaceName), nil)
			if err != nil {
				return err
			}
			logs.Info("Dropped existing space: %s", spaceName)
			return nil
		}
	}
	return nil
}

const defaultBatchSize = 1000

// CopySpace copies all data from srcSpace to dstSpace
func CopySpace(nsid, srcSpace, dstSpace string, force bool, partitionNum, replicaFactor int, vidType string) error {
	logs.Info("Starting to copy space from %s to %s (force=%v, partition_num=%v, replica_factor=%v)", srcSpace, dstSpace, force, partitionNum, replicaFactor)
	// 如果 force 为 true，先检查并删除已存在的 space
	if force {
		if err := dropSpaceIfExists(nsid, dstSpace); err != nil {
			return fmt.Errorf("failed to drop existing space: %w", err)
		}
	}

	err := createSpace(nsid, srcSpace, dstSpace, partitionNum, replicaFactor, vidType)
	if err != nil {
		return fmt.Errorf("failed to create space: %v", err)
	}

	// Wait for space to be ready (metadata sync)
	logs.Info("Waiting for space to be ready...")
	err = waitForSpaceReady(nsid, dstSpace)
	if err != nil {
		return fmt.Errorf("space not ready: %v", err)
	}
	logs.Info("Space is ready")

	err = createIndexes(nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %v", err)
	}

	haveListeners, err := copyListeners(nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to copy listeners: %w", err)
	}
	if haveListeners {
		if err := copyFulltextIndexes(nsid, srcSpace, dstSpace); err != nil {
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

	err = copyVertices(nsid, srcSpace, dstSpace, tags)
	if err != nil {
		return fmt.Errorf("failed to copy vertices: %v", err)
	}

	err = copyEdges(nsid, srcSpace, dstSpace, edges)
	if err != nil {
		return fmt.Errorf("failed to copy edges: %v", err)
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
		result, _, err := dao.Execute(nsid, "SHOW SPACES", nil)
		if err != nil {
			continue
		}
		for _, row := range result.Tables {
			if name, ok := row["Name"].(string); ok {
				if name == spaceName {
					return nil // Space is ready
				}
			}
		}
	}
	return fmt.Errorf("space %s not ready after %d seconds", spaceName, maxRetries)
}

func createSpace(nsid, srcSpace, dstSpace string, partitionNum, replicaFactor int, vidType string) error {
	// 判断使用哪种流程
	if partitionNum == 0 && replicaFactor == 0 && vidType == "" {
		// 原流程: CREATE SPACE dstSpace AS srcSpace
		gql := fmt.Sprintf("CREATE SPACE %s AS %s", dstSpace, srcSpace)
		_, _, err := dao.Execute(nsid, gql, nil)
		return err
	}

	// 新流程: 获取源 space 元数据并构建完整 CREATE SPACE 语句
	desc, err := getSpaceDesc(nsid, srcSpace)
	if err != nil {
		return fmt.Errorf("failed to get space desc: %w", err)
	}

	// 构建 CREATE SPACE 语句
	gql := buildCreateSpaceGql(dstSpace, desc, partitionNum, replicaFactor, vidType)
	_, _, err = dao.Execute(nsid, gql, nil)
	if err != nil {
		return fmt.Errorf("failed to create space: %w", err)
	}

	// 复制 tag schema
	if err := copyTagsAdvanced(nsid, srcSpace, dstSpace); err != nil {
		return fmt.Errorf("failed to copy tags: %w", err)
	}

	// 复制 edge schema
	if err := copyEdgesAdvanced(nsid, srcSpace, dstSpace); err != nil {
		return fmt.Errorf("failed to copy edges: %w", err)
	}

	return nil
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
func copyTagsAdvanced(nsid, srcSpace, dstSpace string) error {
	gql := fmt.Sprintf("USE %s; SHOW TAGS", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return err
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
			dao.Execute(nsid, execGql, nil)
		}
	}

	return nil
}

// copyEdgesAdvanced 逐个复制 edge schema
func copyEdgesAdvanced(nsid, srcSpace, dstSpace string) error {
	gql := fmt.Sprintf("USE %s; SHOW EDGES", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return err
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
			dao.Execute(nsid, execGql, nil)
		}
	}

	return nil
}

func getTags(nsid, space string) ([]string, error) {
	gql := fmt.Sprintf("USE %s; SHOW TAGS", space)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return nil, err
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
		return nil, err
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
	result, _, err := dao.Execute(nsid, fmt.Sprintf("DESCRIBE SPACE %s", spaceName), nil)
	if err != nil {
		return 0, err
	}

	for _, row := range result.Tables {
		if id, ok := row["ID"].(int64); ok {
			return id, nil
		}
	}
	return 0, fmt.Errorf("space ID not found for space %s", spaceName)
}

// copyFulltextIndexes 复制全文本索引到新 space
func copyFulltextIndexes(nsid, srcSpace, dstSpace string) error {
	// 1. 获取 dstSpace 的 ID
	dstSpaceID, err := getSpaceID(nsid, dstSpace)
	if err != nil {
		return err
	}

	// 2. 获取源 space 的全文本索引配置
	gql := fmt.Sprintf("USE %s; SHOW FULLTEXT INDEXES", srcSpace)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return err
	}

	if len(result.Tables) == 0 {
		logs.Info("No fulltext indexes found in source space")
		return nil
	}

	for _, row := range result.Tables {
		var newIndexName string
		if indexName, ok := row["Name"].(string); ok {
			// 为索引名添加 space ID 后缀以避免冲突
			newIndexName = fmt.Sprintf("%s_%d", indexName, dstSpaceID)

			// 获取原索引的创建语句
			descGql := fmt.Sprintf("USE %s; SHOW CREATE TAG INDEX %s", srcSpace, indexName)
			descResult, _, err := dao.Execute(nsid, descGql, nil)
			if err != nil {
				logs.Warn("Failed to get fulltext index %s: %v", indexName, err)
				continue
			}

			if len(descResult.Tables) == 0 {
				continue
			}

			// 解析创建语句
			var createStmt string
			for _, col := range descResult.Headers {
				if strings.Contains(col, "Create") {
					if stmt, ok := descResult.Tables[0][col].(string); ok {
						createStmt = stmt
						break
					}
				}
			}

			if createStmt == "" {
				continue
			}

			// 修改索引名
			newCreateStmt := strings.Replace(createStmt, indexName, newIndexName, 1)

			logs.Info("Creating fulltext index: %s (original: %s)", newIndexName, indexName)

			// 在目标 space 创建索引
			execGql := fmt.Sprintf("USE %s; %s", dstSpace, newCreateStmt)
			if _, _, err := dao.Execute(nsid, execGql, nil); err != nil {
				logs.Warn("Failed to create fulltext index %s: %v", newIndexName, err)
			}
		}
	}

	logs.Info("Fulltext indexes copied successfully")
	return nil
}

// copyFulltextIndex 复制单个全文本索引
func copyFulltextIndex(nsid, srcSpace, dstSpace string, indexName string) error {
	// 获取原索引的创建语句
	descGql := fmt.Sprintf("USE %s; SHOW CREATE TAG INDEX %s", srcSpace, indexName)
	result, _, err := dao.Execute(nsid, descGql, nil)
	if err != nil || len(result.Tables) == 0 {
		return err
	}

	// 解析创建语句
	var createStmt string
	for _, col := range result.Headers {
		if strings.Contains(col, "Create") {
			if stmt, ok := result.Tables[0][col].(string); ok {
				createStmt = stmt
				break
			}
		}
	}

	if createStmt == "" {
		return fmt.Errorf("cannot find create statement for index %s", indexName)
	}

	// 在目标 space 创建索引
	execGql := fmt.Sprintf("USE %s; %s", dstSpace, createStmt)
	_, _, err = dao.Execute(nsid, execGql, nil)
	return err
}

func copyListeners(nsid, srcSpace, dstSpace string) (bool, error) {
	// 获取 listener 配置
	result, _, err := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW LISTENER", srcSpace), nil)
	if err != nil {
		return false, err
	}

	if len(result.Tables) == 0 {
		logs.Info("No listeners found in source space")
		return false, nil
	}

	for _, row := range result.Tables {
		if listenerType, ok := row["Type"].(string); ok {
			if host, ok := row["Host"].(string); ok {
				// 构建创建 listener 的语句
				// 格式: CREATE LISTENER ELASTICSEARCH ON SPACE xxx TO "http://host:port"
				listenerGql := fmt.Sprintf("USE %s; CREATE LISTENER %s ON %s TO \"%s\"",
					dstSpace, listenerType, dstSpace, host)
				_, _, err := dao.Execute(nsid, listenerGql, nil)
				if err != nil {
					logs.Warn("Failed to create listener: %v", err)
				}
			}
		}
	}

	return true, nil
}

func createIndexes(nsid, srcSpace, dstSpace string) error {
	tagIndexResult, _, _ := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW TAG INDEXES", srcSpace), nil)
	edgeIndexResult, _, _ := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW EDGE INDEXES", srcSpace), nil)

	for _, row := range tagIndexResult.Tables {
		if indexName, ok := row["Index Name"].(string); ok {
			createTagIndex(nsid, srcSpace, dstSpace, indexName)
		}
	}
	for _, row := range edgeIndexResult.Tables {
		if indexName, ok := row["Index Name"].(string); ok {
			createEdgeIndex(nsid, srcSpace, dstSpace, indexName)
		}
	}
	return nil
}

func createTagIndex(nsid, srcSpace, dstSpace, indexName string) error {
	result, _, err := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW CREATE TAG INDEX %s", srcSpace, indexName), nil)
	if err != nil || len(result.Tables) == 0 {
		return err
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
	_, _, err = dao.Execute(nsid, fmt.Sprintf("USE %s; %s", dstSpace, createStmt), nil)
	return err
}

func createEdgeIndex(nsid, srcSpace, dstSpace, indexName string) error {
	result, _, err := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW CREATE EDGE INDEX %s", srcSpace, indexName), nil)
	if err != nil || len(result.Tables) == 0 {
		return err
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
	_, _, err = dao.Execute(nsid, fmt.Sprintf("USE %s; %s", dstSpace, createStmt), nil)
	return err
}

func copyVertices(nsid, srcSpace, dstSpace string, tags []string) error {
	for _, tag := range tags {
		logs.Info("Copying vertices for tag: %s", tag)
		scanner, err := NewStorageScanner(nsid, srcSpace)
		if err != nil {
			return err
		}
		defer scanner.Close()

		// 扫描源 space 的 vertex
		err = scanner.ScanVertices(tag, defaultBatchSize, func(vertices []map[string]interface{}) error {
			logs.Info("Scanned %d vertices for tag %s", len(vertices), tag)
			// 批量插入到目标 space
			if err := insertVertexBatch(nsid, dstSpace, tag, vertices); err != nil {
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

func insertVertexBatch(nsid, dstSpace, tag string, vertices []map[string]interface{}) error {
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

	logs.Info("Insert vertex GQL: %s", insertGql)
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

func copyEdges(nsid, srcSpace, dstSpace string, edges []string) error {

	for _, edge := range edges {
		logs.Info("Copying edges for type: %s", edge)
		scanner, err := NewStorageScanner(nsid, srcSpace)
		if err != nil {
			return err
		}
		defer scanner.Close()

		// 扫描源 space 的 edge
		err = scanner.ScanEdges(edge, defaultBatchSize, func(edges []map[string]interface{}) error {
			logs.Info("Scanned %d edges for type %s", len(edges), edge)
			// 批量插入到目标 space
			if err := insertEdgeBatch(nsid, dstSpace, edge, edges); err != nil {
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

func insertEdgeBatch(nsid, dstSpace, edge string, edges []map[string]interface{}) error {
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

	logs.Info("Insert edge GQL: %s", insertGql)
	_, _, err := executeWithRetry(nsid, insertGql)
	if err != nil {
		logs.Error("Insert edge failed: %v", err)
		return err
	}

	return nil
}
