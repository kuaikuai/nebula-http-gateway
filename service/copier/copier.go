package copier

import (
	"fmt"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/dao"
)

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
			logs.Info("[DEBUG] Dropped existing space: %s", spaceName)
			return nil
		}
	}
	return nil
}

const defaultBatchSize = 1000

// CopySpace copies all data from srcSpace to dstSpace
func CopySpace(nsid, srcSpace, dstSpace string, force bool) error {
	// 如果 force 为 true，先检查并删除已存在的 space
	if force {
		if err := dropSpaceIfExists(nsid, dstSpace); err != nil {
			return fmt.Errorf("failed to drop existing space: %w", err)
		}
	}

err := createSpace(nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to create space: %v", err)
	}

	// Wait for space to be ready (metadata sync)
	fmt.Println("[INFO] Waiting for space to be ready...")
	err = waitForSpaceReady(nsid, dstSpace)
	if err != nil {
		return fmt.Errorf("space not ready: %v", err)
	}
	fmt.Println("[INFO] Space is ready")

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

func createSpace(nsid, srcSpace, dstSpace string) error {
	gql := fmt.Sprintf("CREATE SPACE %s AS %s", dstSpace, srcSpace)
	_, _, err := dao.Execute(nsid, gql, nil)
	return err
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
		return fmt.Errorf("failed to get dst space ID: %w", err)
	}

	// 2. 在原 space 获取全文本索引信息
	result, _, err := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW FULLTEXT INDEXES", srcSpace), nil)
	if err != nil {
		return fmt.Errorf("show fulltext indexes failed: %w", err)
	}

	if len(result.Tables) == 0 {
		logs.Info("[DEBUG] No fulltext indexes found in source space")
		return nil
	}

	// 3. 解析并创建全文本索引
	for _, row := range result.Tables {
		indexName, _ := row["Name"].(string)
		schemaType, _ := row["Schema Type"].(string)  // Tag 或 Edge
		schemaName, _ := row["Schema Name"].(string)
		fields, _ := row["Fields"].(string)
		analyzer, _ := row["Analyzer"].(string)

		if indexName == "" || schemaName == "" || fields == "" {
			continue
		}

		// 在索引名后加上 space ID 避免冲突
		newIndexName := fmt.Sprintf("%s_%d", indexName, dstSpaceID)

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

		logs.Info("[DEBUG] Creating fulltext index: %s (original: %s)", newIndexName, indexName)

		_, _, err = dao.Execute(nsid, fullGql, nil)
		if err != nil {
			return fmt.Errorf("create fulltext index %s failed: %w", newIndexName, err)
		}
	}

	logs.Info("[DEBUG] Fulltext indexes copied successfully")
	return nil
}

// copyListeners 复制 LISTENER 配置到新 space
func copyListeners(nsid, srcSpace, dstSpace string) (bool,error) {
	// 1. 在原 space 获取 LISTENER 信息
	result, _, err := dao.Execute(nsid, fmt.Sprintf("USE %s; SHOW LISTENER", srcSpace), nil)
	if err != nil {
		return false, fmt.Errorf("show listener failed: %w", err)
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

	_, _, err = dao.Execute(nsid, fullGql, nil)
	if err != nil {
		return false, fmt.Errorf("add listener failed: %w", err)
	}

	logs.Info("Listener copied successfully")
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
	scanner, err := NewStorageScanner(nsid, srcSpace)
	if err != nil {
		return fmt.Errorf("failed to create storage scanner: %v", err)
	}
	defer scanner.Close()

	for _, tag := range tags {
		err := scanAndCopyVerticesWithStorage(scanner, nsid, dstSpace, tag)
		if err != nil {
			return fmt.Errorf("failed to copy vertices for tag %s: %v", tag, err)
		}
	}
	return nil
}

func scanAndCopyVerticesWithStorage(scanner *StorageScanner, nsid, dstSpace, tag string) error {
	return scanner.ScanVertices(tag, defaultBatchSize, func(vertices []map[string]interface{}) error {
		return insertVertexBatch(nsid, dstSpace, tag, vertices)
	})
}

func insertVertexBatch(nsid, dstSpace, tag string, vertices []map[string]interface{}) error {
	if len(vertices) == 0 {
		return nil
	}

	var allKeys []string
	keySet := make(map[string]bool)
	for _, v := range vertices {
		for k := range v {
			if !keySet[k] && k != "vid" && k != "v" && k != "type" {
				keySet[k] = true
				allKeys = append(allKeys, k)
			}
		}
	}

	if len(allKeys) == 0 {
		var valueParts []string
		for _, v := range vertices {
			if vid, ok := v["vid"]; ok {
				valueParts = append(valueParts, fmt.Sprintf("%s:()", formatVid(vid)))
			}
		}
		if len(valueParts) == 0 {
			return nil
		}
		insertGql := fmt.Sprintf("USE %s; INSERT VERTEX %s() VALUES %s",
			dstSpace, tag, strings.Join(valueParts, ", "))
		fmt.Printf("[DEBUG] Insert vertex GQL: %s\n", insertGql)
		_, _, err := dao.Execute(nsid, insertGql, nil)
		if err != nil {
			fmt.Printf("[ERROR] Insert vertex failed: %v\n", err)
		}
		return err
	}

	var valueParts []string
	for _, v := range vertices {
		vid := v["vid"]
		if vid == nil {
			continue
		}
		var values []string
		for _, k := range allKeys {
			if val, ok := v[k]; ok {
				values = append(values, formatValue(val))
			} else {
				values = append(values, "null")
			}
		}
		valueParts = append(valueParts, fmt.Sprintf("%s:(%s)", formatVid(vid), strings.Join(values, ", ")))
	}

	if len(valueParts) == 0 {
		return nil
	}

	propList := strings.Join(allKeys, ", ")
	insertGql := fmt.Sprintf("USE %s; INSERT VERTEX %s(%s) VALUES %s",
		dstSpace, tag, propList, strings.Join(valueParts, ", "))
	fmt.Printf("[DEBUG] Insert vertex GQL: %s\n", insertGql)
	_, _, err := dao.Execute(nsid, insertGql, nil)
	if err != nil {
		fmt.Printf("[ERROR] Insert vertex failed: %v\n", err)
	}
	return err
}

func copyEdges(nsid, srcSpace, dstSpace string, edges []string) error {
	scanner, err := NewStorageScanner(nsid, srcSpace)
	if err != nil {
		return fmt.Errorf("failed to create storage scanner: %v", err)
	}
	defer scanner.Close()

	for _, edge := range edges {
		err := scanAndCopyEdgesWithStorage(scanner, nsid, dstSpace, edge)
		if err != nil {
			return fmt.Errorf("failed to copy edges for %s: %v", edge, err)
		}
	}
	return nil
}

func scanAndCopyEdgesWithStorage(scanner *StorageScanner, nsid, dstSpace, edge string) error {
	return scanner.ScanEdges(edge, defaultBatchSize, func(edges []map[string]interface{}) error {
		return insertEdgeBatch(nsid, dstSpace, edge, edges)
	})
}

func insertEdgeBatch(nsid, dstSpace, edge string, edges []map[string]interface{}) error {
	if len(edges) == 0 {
		return nil
	}

	var allKeys []string
	keySet := make(map[string]bool)
	for _, e := range edges {
		for k := range e {
			if !keySet[k] && k != "_src" && k != "_dst" && k != "_rank" && k != "_e" && k != "_type" {
				keySet[k] = true
				allKeys = append(allKeys, k)
			}
		}
	}

	if len(allKeys) == 0 {
		var valueParts []string
		for _, e := range edges {
			srcID := e["_src"]
			dstID := e["_dst"]
			if srcID == nil || dstID == nil {
				continue
			}
			valueParts = append(valueParts, fmt.Sprintf("%s->%s:()", formatVid(srcID), formatVid(dstID)))
		}
		if len(valueParts) == 0 {
			return nil
		}
		insertGql := fmt.Sprintf("USE %s; INSERT EDGE %s() VALUES %s",
			dstSpace, edge, strings.Join(valueParts, ", "))
		fmt.Printf("[DEBUG] Insert edge GQL: %s\n", insertGql)
		_, _, err := dao.Execute(nsid, insertGql, nil)
		if err != nil {
			fmt.Printf("[ERROR] Insert edge failed: %v\n", err)
		}
		return err
	}
	fmt.Printf("[DEBUG] edge allkeys:%v\n", allKeys)
	propList := strings.Join(allKeys, ", ")
	var valueParts []string
	for _, e := range edges {
		srcID := e["_src"]
		dstID := e["_dst"]
		if srcID == nil || dstID == nil {
			continue
		}
		var values []string
		for _, k := range allKeys {
			if val, ok := e[k]; ok {
				values = append(values, formatValue(val))
			} else {
				values = append(values, "null")
			}
		}
		valueParts = append(valueParts, fmt.Sprintf("%s->%s:(%s)", formatVid(srcID), formatVid(dstID), strings.Join(values, ", ")))
	}

	if len(valueParts) == 0 {
		return nil
	}

	insertGql := fmt.Sprintf("USE %s; INSERT EDGE %s(%s) VALUES %s",
		dstSpace, edge, propList, strings.Join(valueParts, ", "))
	fmt.Printf("[DEBUG] Insert edge GQL: %s\n", insertGql)
	_, _, err := dao.Execute(nsid, insertGql, nil)
	if err != nil {
		fmt.Printf("[ERROR] Insert edge failed: %v\n", err)
	}
	return err
}

// formatVid formats the vertex/edge ID for use in INSERT statements
func formatVid(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", escapeString(val))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	default:
		// Fallback: convert to string and quote
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
	// Order matters: escape backslash first, then quotes
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return s
}
