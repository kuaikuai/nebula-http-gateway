package copier

import (
	"fmt"
	"strings"

	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/dao"
)

const defaultBatchSize = 1000

// CopySpace copies all data from srcSpace to dstSpace
func CopySpace(nsid, srcSpace, dstSpace string) error {
	err := createSpace(nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to create space: %v", err)
	}

	err = createIndexes(nsid, srcSpace, dstSpace)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %v", err)
	}

	//TODO: ES indexes are not supported by storage API, so we need to create them before copying data

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
				valueParts = append(valueParts, fmt.Sprintf("\"%v\":()", vid))
			}
		}
		if len(valueParts) == 0 {
			return nil
		}
		insertGql := fmt.Sprintf("USE %s; INSERT VERTEX %s() VALUES %s",
			dstSpace, tag, strings.Join(valueParts, ", "))
		fmt.Println("[DEBUG] INSERT VERTEX (no props):", insertGql)
		_, _, err := dao.Execute(nsid, insertGql, nil)
		return err
	}

	var valueParts []string
	for _, v := range vertices {
		vid := v["vid"]
		if vid == nil {
			continue
		}
		fmt.Printf("[DEBUG] Vertex vid: %v, allKeys: %v\n", vid, allKeys)
		var values []string
		for _, k := range allKeys {
			if val, ok := v[k]; ok {
				values = append(values, formatValue(val))
			} else {
				values = append(values, "null")
			}
		}
		valueParts = append(valueParts, fmt.Sprintf("\"%v\":(%s)", vid, strings.Join(values, ", ")))
	}

	if len(valueParts) == 0 {
		return nil
	}

	propList := strings.Join(allKeys, ", ")
	insertGql := fmt.Sprintf("USE %s; INSERT VERTEX %s(%s) VALUES %s",
		dstSpace, tag, propList, strings.Join(valueParts, ", "))
	fmt.Println("[DEBUG] INSERT VERTEX:", insertGql)
	_, _, err := dao.Execute(nsid, insertGql, nil)
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
			if !keySet[k] && k != "src" && k != "dst" && k != "rank" && k != "e" && k != "type" {
				keySet[k] = true
				allKeys = append(allKeys, k)
			}
		}
	}

	if len(allKeys) == 0 {
		var valueParts []string
		for _, e := range edges {
			srcID := e["src"]
			dstID := e["dst"]
			if srcID == nil || dstID == nil {
				continue
			}
			valueParts = append(valueParts, fmt.Sprintf("\"%v\"->\"%v\"", srcID, dstID))
		}
		if len(valueParts) == 0 {
			return nil
		}
		insertGql := fmt.Sprintf("USE %s; INSERT EDGE %s() VALUES %s",
			dstSpace, edge, strings.Join(valueParts, ", "))
		_, _, err := dao.Execute(nsid, insertGql, nil)
		return err
	}

	propList := strings.Join(allKeys, ", ")
	var valueParts []string
	for _, e := range edges {
		srcID := e["src"]
		dstID := e["dst"]
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
		valueParts = append(valueParts, fmt.Sprintf("\"%v\"->\"%v\":(%s)", srcID, dstID, strings.Join(values, ", ")))
	}

	if len(valueParts) == 0 {
		return nil
	}

	insertGql := fmt.Sprintf("USE %s; INSERT EDGE %s(%s) VALUES %s",
		dstSpace, edge, propList, strings.Join(valueParts, ", "))
	_, _, err := dao.Execute(nsid, insertGql, nil)
	return err
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(val, "\"", "\\\""))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
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
