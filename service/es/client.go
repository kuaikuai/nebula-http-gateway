package es

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/dao"
)

// ESClient ES 客户端
type ESClient struct {
	addresses []string
	username  string
	password  string
	client    *http.Client
}

// ESBulkDoc ES 批量写入文档
type ESBulkDoc struct {
	Index string
	DocID string
	VID   string
	Src   string
	Dst   string
	Rank  int64
	Props map[string]interface{}
}

// IndexMapping 索引映射
type IndexMapping struct {
	IndexName  string
	SchemaType string // Tag 或 Edge
	SchemaName string
	Fields     string
	Analyzer   string
}

// NewESClient 创建 ES 客户端
func NewESClient(nsid, space, username, password string) (*ESClient, error) {
	// 获取 ES client 配置 - 使用 SHOW TEXT SEARCH CLIENTS
	gql := "SHOW TEXT SEARCH CLIENTS"
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return nil, fmt.Errorf("show text search clients failed: %w", err)
	}

	logs.Info("SHOW TEXT SEARCH CLIENTS result tables count: %d", len(result.Tables))
	if len(result.Tables) > 0 {
		logs.Info("SHOW TEXT SEARCH CLIENTS first row: %+v", result.Tables[0])
	}

	var addresses []string
	for _, row := range result.Tables {
		if host, ok := row["Host"].(string); ok {
			// 移除所有引号
			host = strings.ReplaceAll(host, "\"", "")
			if port, ok := row["Port"].(int64); ok {
				host = fmt.Sprintf("%s:%d", host, port)
			}
			logs.Info("Found ES client: %s", host)
			addresses = append(addresses, host)
		}
	}

	if len(addresses) == 0 {
		// 返回更详细的错误信息
		if len(result.Tables) == 0 {
			return nil, fmt.Errorf("no text search clients configured")
		}
		return nil, fmt.Errorf("no ES client found")
	}

	return &ESClient{
		addresses: addresses,
		username:  username,
		password:  password,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

// GetIndexMapping 获取索引映射
func GetIndexMapping(nsid, space, indexName string) (*IndexMapping, error) {
	gql := fmt.Sprintf("USE %s; SHOW FULLTEXT INDEXES", space)
	result, _, err := dao.Execute(nsid, gql, nil)
	if err != nil {
		return nil, fmt.Errorf("show fulltext indexes failed: %w", err)
	}

	for _, row := range result.Tables {
		name, _ := row["Name"].(string)
		if name == indexName {
			return &IndexMapping{
				IndexName:  name,
				SchemaType: row["Schema Type"].(string),
				SchemaName: row["Schema Name"].(string),
				Fields:     row["Fields"].(string),
				Analyzer:   row["Analyzer"].(string),
			}, nil
		}
	}

	return nil, fmt.Errorf("index %s not found", indexName)
}

// genDocID 生成 ES 文档 ID，与 NebulaGraph 官方保持一致
func genDocID(vid, src, dst string, rank int64) string {
	var str string
	if vid != "" {
		str = vid
	} else {
		str = src + dst + fmt.Sprintf("%d", rank)
	}

	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:])
}

// BuildDocID 生成文档 ID
func BuildDocID(vid, src, dst string, rank int64) string {
	return genDocID(vid, src, dst, rank)
}

// BulkWrite 批量写入（带重试）
func (c *ESClient) BulkWrite(ctx context.Context, docs []ESBulkDoc) error {
	if len(docs) == 0 || len(c.addresses) == 0 {
		return nil
	}

	// 构建 bulk 请求体
	var bulkBody strings.Builder
	for _, doc := range docs {
		// index action
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": doc.Index,
				"_id":    doc.DocID,
			},
		}
		actionJSON, _ := json.Marshal(action)
		bulkBody.Write(actionJSON)
		bulkBody.WriteString("\n")

		// document body
		body := map[string]interface{}{
			"vid":  doc.VID,
			"src":  doc.Src,
			"dst":  doc.Dst,
			"rank": doc.Rank,
		}
		for k, v := range doc.Props {
			body[k] = v
		}
		bodyJSON, _ := json.Marshal(body)
		bulkBody.Write(bodyJSON)
		bulkBody.WriteString("\n")
	}

	// 重试机制
	maxRetries := 6
	retryInterval := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logs.Warn("Retry bulk write, attempt %d/%d", attempt+1, maxRetries)
			time.Sleep(retryInterval)
			retryInterval *= 2 // 指数退避
		}

		err := c.doBulkWrite(ctx, bulkBody.String())
		if err == nil {
			return nil
		}

		// 判断是否为可重试错误
		if !isRetryableError(err) {
			return err
		}
		lastErr = err
		logs.Warn("Bulk write failed (retryable): %v", err)
	}

	return fmt.Errorf("bulk write failed after %d retries: %w", maxRetries, lastErr)
}

// isRetryableError 判断错误是否可重试
func isRetryableError(err error) bool {
	errStr := strings.ToLower(err.Error())
	// 超时、连接、网络错误可重试
	retryableKeywords := []string{
		"timeout",
		"deadline",
		"i/o timeout",
		"connection refused",
		"no route to host",
		"network is unreachable",
		"temporary failure",
		"503",
		"502",
		"429", // too many requests
	}
	for _, keyword := range retryableKeywords {
		if strings.Contains(errStr, keyword) {
			return true
		}
	}
	return false
}

// doBulkWrite 执行实际的 bulk 写入
func (c *ESClient) doBulkWrite(ctx context.Context, body string) error {
	// 确保地址有 http:// 前缀
	esAddr := c.addresses[0]
	if !strings.HasPrefix(esAddr, "http://") && !strings.HasPrefix(esAddr, "https://") {
		esAddr = "http://" + esAddr
	}
	url := fmt.Sprintf("%s/_bulk", esAddr)

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	// 添加 Basic Auth 认证
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("execute bulk failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bulk write failed, status: %d, body: %s", resp.StatusCode, string(respBody))
	}

	// 解析响应检查错误
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil // 成功
	}

	if errors, ok := result["errors"].(bool); ok && errors {
		logs.Warn("ES bulk write has errors: %v", result)
	}

	return nil
}

// Close 关闭客户端
func (c *ESClient) Close() {
	// http.Client 不需要显式关闭
}
