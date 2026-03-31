package es

import (
	"context"
	"fmt"
	"strings"

	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-http-gateway/service/copier"
)

// Syncer 同步器
type Syncer struct {
	nsid      string
	space     string
	esIndex   string
	esClient  *ESClient
	batchSize int
}

// NewSyncer 创建同步器
func NewSyncer(nsid, space, esIndex string, batchSize int, username, password string) (*Syncer, error) {
	esClient, err := NewESClient(nsid, space, username, password)
	if err != nil {
		return nil, err
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &Syncer{
		nsid:      nsid,
		space:     space,
		esIndex:   esIndex,
		esClient:  esClient,
		batchSize: batchSize,
	}, nil
}

// Sync 同步数据
func (s *Syncer) Sync(ctx context.Context) (int64, error) {
	// 获取索引映射
	mapping, err := GetIndexMapping(s.nsid, s.space, s.esIndex)
	if err != nil {
		return 0, err
	}

	logs.Info("Syncing index %s, schema: %s %s, fields: %s",
		s.esIndex, mapping.SchemaType, mapping.SchemaName, mapping.Fields)

	var totalCount int64
	var syncErr error

	if mapping.SchemaType == "Tag" {
		totalCount, syncErr = s.syncVertices(ctx, mapping)
	} else if mapping.SchemaType == "Edge" {
		totalCount, syncErr = s.syncEdges(ctx, mapping)
	} else {
		return 0, fmt.Errorf("unknown schema type: %s", mapping.SchemaType)
	}

	return totalCount, syncErr
}

func (s *Syncer) syncVertices(ctx context.Context, mapping *IndexMapping) (int64, error) {
	scanner, err := copier.NewStorageScanner(s.nsid, s.space)
	if err != nil {
		return 0, fmt.Errorf("failed to create storage scanner: %w", err)
	}
	defer scanner.Close()

	// 解析 fields - 可能是单个字段如 "name" 或多个字段
	fields := strings.Split(mapping.Fields, ",")
	var fieldSet = make(map[string]bool)
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			fieldSet[f] = true
		}
	}
	logs.Info("Filtering fields: %v", fieldSet)

	var count int64
	var batch []ESBulkDoc

	err = scanner.ScanVertices(mapping.SchemaName, s.batchSize, func(vertices []map[string]interface{}) error {
		for _, v := range vertices {
			vidVal := v["vid"]
			if vidVal == nil {
				continue
			}
			vid := fmt.Sprintf("%v", vidVal)

			docID := genDocID(vid, "", "", 0)

			// 只包含 fields 中指定的属性
			props := make(map[string]interface{})
			for k, val := range v {
				if k == "vid" {
					continue
				}
				// 只添加 mapping 中指定的字段
				if fieldSet[k] {
					props[k] = val
				}
			}

			batch = append(batch, ESBulkDoc{
				Index: s.esIndex,
				DocID: docID,
				VID:   vid,
				Props: props,
			})
		}

		if len(batch) >= s.batchSize {
			if err := s.esClient.BulkWrite(ctx, batch); err != nil {
				logs.Error("Failed to bulk write vertices: %v", err)
				return err
			}
			count += int64(len(batch))
			logs.Info("Synced %d vertices, total: %d", len(batch), count)
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		return count, err
	}

	// 处理剩余数据
	if len(batch) > 0 {
		if err := s.esClient.BulkWrite(ctx, batch); err != nil {
			logs.Error("Failed to bulk write remaining vertices: %v", err)
			return count, err
		}
		count += int64(len(batch))
		logs.Info("Synced remaining %d vertices, total: %d", len(batch), count)
	}

	return count, nil
}

func (s *Syncer) syncEdges(ctx context.Context, mapping *IndexMapping) (int64, error) {
	scanner, err := copier.NewStorageScanner(s.nsid, s.space)
	if err != nil {
		return 0, fmt.Errorf("failed to create storage scanner: %w", err)
	}
	defer scanner.Close()

	// 解析 fields - 可能是单个字段如 "name" 或多个字段
	fields := strings.Split(mapping.Fields, ",")
	var fieldSet = make(map[string]bool)
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			fieldSet[f] = true
		}
	}
	logs.Info("Filtering fields: %v", fieldSet)

	var count int64
	var batch []ESBulkDoc

	err = scanner.ScanEdges(mapping.SchemaName, s.batchSize, func(edges []map[string]interface{}) error {
		for _, e := range edges {
			srcVal := e["_src"]
			dstVal := e["_dst"]
			rankVal := e["_rank"]

			if srcVal == nil || dstVal == nil {
				continue
			}

			src := fmt.Sprintf("%v", srcVal)
			dst := fmt.Sprintf("%v", dstVal)
			var rank int64
			if rankVal != nil {
				rank = int64(rankVal.(float64))
			}

			docID := genDocID("", src, dst, rank)

			// 只包含 fields 中指定的属性
			props := make(map[string]interface{})
			for k, val := range e {
				if k == "_src" || k == "_dst" || k == "_rank" {
					continue
				}
				// 只添加 mapping 中指定的字段
				if fieldSet[k] {
					props[k] = val
				}
			}

			batch = append(batch, ESBulkDoc{
				Index: s.esIndex,
				DocID: docID,
				Src:   src,
				Dst:   dst,
				Rank:  rank,
				Props: props,
			})
		}

		if len(batch) >= s.batchSize {
			if err := s.esClient.BulkWrite(ctx, batch); err != nil {
				logs.Error("Failed to bulk write edges: %v", err)
				return err
			}
			count += int64(len(batch))
			logs.Info("Synced %d edges, total: %d", len(batch), count)
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		return count, err
	}

	// 处理剩余数据
	if len(batch) > 0 {
		if err := s.esClient.BulkWrite(ctx, batch); err != nil {
			logs.Error("Failed to bulk write remaining edges: %v", err)
			return count, err
		}
		count += int64(len(batch))
		logs.Info("Synced remaining %d edges, total: %d", len(batch), count)
	}

	return count, nil
}
