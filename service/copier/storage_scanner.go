package copier

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/storage"
	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/dao"
	"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/pool"
)

type StorageScanner struct {
	nsid        string
	storageAddr string
	client      *storage.GraphStorageServiceClient
	spaceID     nebula.GraphSpaceID
	spaceName   string
	partIDs     []nebula.PartitionID
	sessionID   int64
}

func NewStorageScanner(nsid, spaceName string) (*StorageScanner, error) {
	scanner := &StorageScanner{
		nsid:      nsid,
		spaceName: spaceName,
	}

	sessionID, err := pool.GetSessionID(nsid)
	if err != nil {
		return nil, fmt.Errorf("failed to get session ID: %w", err)
	}
	scanner.sessionID = sessionID
	logs.Info("[DEBUG] SessionID: %v", sessionID)

	addrs, err := scanner.getStorageAddresses()
	if err != nil {
		return nil, fmt.Errorf("failed to get storage addresses: %w", err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no storage addresses available")
	}
	scanner.storageAddr = addrs[0]

	spaceID, err := scanner.getSpaceID(spaceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get space ID: %w", err)
	}
	scanner.spaceID = spaceID
	logs.Info("[DEBUG] SpaceID: %v", spaceID)

	partIDs, err := scanner.getPartitionIDs()
	if err != nil {
		return nil, fmt.Errorf("failed to get partition IDs: %w", err)
	}
	if len(partIDs) == 0 {
		return nil, fmt.Errorf("no partitions found")
	}
	scanner.partIDs = partIDs

	client, err := scanner.createStorageClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	scanner.client = client

	return scanner, nil
}

func (s *StorageScanner) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *StorageScanner) getStorageAddresses() ([]string, error) {
	gql := fmt.Sprintf("USE %s; SHOW HOSTS STORAGE", s.spaceName)
	result, _, err := dao.Execute(s.nsid, gql, nil)
	if err != nil {
		return nil, err
	}

	var addrs []string
	for _, row := range result.Tables {
		if host, ok := row["Host"].(string); ok {
			if port, ok := row["Port"].(int64); ok {
				addrs = append(addrs, fmt.Sprintf("%s:%d", host, port))
			}
		}
	}
	return addrs, nil
}

func (s *StorageScanner) getPartitionIDs() ([]nebula.PartitionID, error) {
	result, _, err := dao.Execute(s.nsid, fmt.Sprintf("USE %s; SHOW PARTS", s.spaceName), nil)
	if err != nil {
		return nil, err
	}

	var partIDs []nebula.PartitionID
	for _, row := range result.Tables {
		if partID, ok := row["Partition ID"].(int64); ok {
			partIDs = append(partIDs, nebula.PartitionID(partID))
		}
	}
	return partIDs, nil
}

func (s *StorageScanner) getSpaceID(spaceName string) (nebula.GraphSpaceID, error) {
	result, _, err := dao.Execute(s.nsid, fmt.Sprintf("DESCRIBE SPACE %s", spaceName), nil)
	if err != nil {
		return 0, err
	}

	for _, row := range result.Tables {
		if id, ok := row["ID"].(int64); ok {
			return nebula.GraphSpaceID(id), nil
		}
	}

	result2, _, err := dao.Execute(s.nsid, "SHOW SPACES", nil)
	if err != nil {
		return 0, err
	}

	for i, row := range result2.Tables {
		if name, ok := row["Name"].(string); ok {
			if name == spaceName {
				return nebula.GraphSpaceID(i + 1), nil
			}
		}
	}
	return 1, nil
}

func (s *StorageScanner) getTagID(tagName string) (nebula.TagID, error) {
	explainGql := fmt.Sprintf("EXPLAIN format='row' MATCH (n:%s) RETURN n LIMIT 1", tagName)
	result, _, err := dao.Execute(s.nsid, explainGql, nil)
	if err != nil {
		return 0, err
	}

	logs.Info("[DEBUG] EXPLAIN result: %v", result.Tables)

	for _, row := range result.Tables {
		if opName, ok := row["name"].(string); ok {
			if opName == "ScanVertices" {
				if opInfo, ok := row["operator info"].(string); ok {
					logs.Info("[DEBUG] ScanVertices operator info: %v", opInfo)
					if tagID, found := extractTagID(opInfo); found {
						return nebula.TagID(tagID), nil
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("tag ID not found for %s", tagName)
}

func extractTagID(opInfo string) (int64, bool) {
	for i := 0; i < len(opInfo)-8; i++ {
		if strings.Contains(opInfo[i:i+7], "tagId") {
			for j := i; j < len(opInfo); j++ {
				if opInfo[j] == ':' {
					j++
					for j < len(opInfo) && (opInfo[j] == ':' || opInfo[j] == ' ' || opInfo[j] == '"') {
						j++
					}
					var num int64
					for j < len(opInfo) && opInfo[j] >= '0' && opInfo[j] <= '9' {
						num = num*10 + int64(opInfo[j]-'0')
						j++
					}
					if num > 0 {
						return num, true
					}
				}
			}
		}
	}
	return 0, false
}

func (s *StorageScanner) getEdgeID(edgeName string) (nebula.EdgeType, error) {
	explainGql := fmt.Sprintf("EXPLAIN format='row' MATCH (a)-[r:%s]->(b) RETURN r LIMIT 1", edgeName)
	result, _, err := dao.Execute(s.nsid, explainGql, nil)
	if err != nil {
		return 0, err
	}

	logs.Info("[DEBUG] EXPLAIN result: %v", result.Tables)

	for _, row := range result.Tables {
		if opName, ok := row["name"].(string); ok {
			if opName == "Traverse" {
				if opInfo, ok := row["operator info"].(string); ok {
					logs.Info("[DEBUG] ScanEdges operator info: %v", opInfo)
					if edgeTypeID, found := extractEdgeTypeID(opInfo); found {
						return nebula.EdgeType(edgeTypeID), nil
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("edge ID not found for %s", edgeName)
}

func extractEdgeTypeID(opInfo string) (int64, bool) {
	re := regexp.MustCompile(`"type":\s*(\d+)`)
	matches := re.FindAllStringSubmatch(opInfo, -1)

	for _, match := range matches {
		if len(match) > 1 {
			num, err := strconv.ParseInt(match[1], 10, 64)
			if err == nil && num > 0 {
				return num, true
			}
		}
	}

	return 0, false
}

func (s *StorageScanner) createStorageClient() (*storage.GraphStorageServiceClient, error) {
	parts := strings.Split(strings.TrimSpace(s.storageAddr), ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid storage address: %s", s.storageAddr)
	}

	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid port: %s", parts[1])
	}

	addrStr := fmt.Sprintf("%s:%d", host, port)
	socket, err := thrift.NewSocket(thrift.SocketAddr(addrStr), thrift.SocketTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to create socket: %w", err)
	}

	bufferSize := 128 << 10
	frameMaxLength := uint32(0x7fffffff)
	bufferedTranFactory := thrift.NewBufferedTransportFactory(bufferSize)
	trans := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(socket), frameMaxLength)

	protocolFactory := thrift.NewBinaryProtocolFactoryDefault()

	client := storage.NewGraphStorageServiceClientFactory(trans, protocolFactory)

	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open storage connection: %w", err)
	}

	return client, nil
}

func (s *StorageScanner) ScanVertices(tagName string, batchSize int, handler func([]map[string]interface{}) error) error {
	logs.Info("[DEBUG] ScanVertices spaceID: %v", s.spaceID)
	logs.Info("[DEBUG] ScanVertices tagName: %v", tagName)

	tagID, err := s.getTagID(tagName)
	if err != nil {
		return fmt.Errorf("failed to get tag ID: %w", err)
	}
	logs.Info("[DEBUG] ScanVertices tagID: %v", tagID)

	parts := make(map[nebula.PartitionID]*storage.ScanCursor)
	for _, pid := range s.partIDs {
		parts[pid] = nil
	}

	batch := make([]map[string]interface{}, 0, batchSize)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := handler(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	sessionIDVal := s.sessionID
	common := &storage.RequestCommon{
		SessionID: &sessionIDVal,
	}

	returnColumns := []*storage.VertexProp{
		{Tag: tagID, Props: nil},
	}

	for {
		req := &storage.ScanVertexRequest{
			SpaceID:                s.spaceID,
			Parts:                  parts,
			ReturnColumns:          returnColumns,
			Limit:                  int64(batchSize),
			OnlyLatestVersion:      true,
			EnableReadFromFollower: true,
			Common:                 common,
		}

		resp, err := s.client.ScanVertex(req)
		if err != nil {
			if isTimeoutError(err) && len(batch) > 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("scan vertex failed: %w", err)
		}

		failedParts := resp.GetResult_().GetFailedParts()
		for _, part := range failedParts {
			if part.GetCode() == nebula.ErrorCode_E_PART_NOT_FOUND {
				continue
			}
			if part.GetCode() != nebula.ErrorCode_SUCCEEDED {
				return fmt.Errorf("scan failed on partition %d: error code %d", part.GetPartID(), part.GetCode())
			}
		}

		vertexData := resp.GetProps()
		if vertexData == nil || len(vertexData.GetRows()) == 0 {
			if err := flushBatch(); err != nil {
				return err
			}
			break
		}

		columnNames := vertexData.GetColumnNames()
		fmt.Printf("[DEBUG] ScanVertices columnNames: %v\n", columnNames)

		for _, row := range vertexData.GetRows() {
			values := row.GetValues()
			fmt.Printf("[DEBUG] ScanVertices first row values: %v\n", values)
			if len(values) == 0 {
				continue
			}

			// Determine if _vid is in columnNames
			vidColumnIndex := -1
			for i, colName := range columnNames {
				if strings.ToLower(string(colName)) == "_vid" || strings.ToLower(string(colName)) == "vid" {
					vidColumnIndex = i
					break
				}
			}

			var vid string
			if vidColumnIndex >= 0 {
				// _vid is in columnNames, extract from that position
				vid = s.valueToString(values[vidColumnIndex])
			} else {
				// _vid is not in columnNames, assume first value is vid
				vid = s.valueToString(values[0])
			}
			fmt.Printf("[DEBUG] Extracted vid: '%s' (column index: %d)\n", vid, vidColumnIndex)
			if vidColumnIndex >= 0 {
				// _vid is in columnNames, extract from that position
				vid = s.valueToString(values[vidColumnIndex])
			} else {
				// _vid is not in columnNames, assume first value is vid
				vid = s.valueToString(values[0])
			}

			propsMap := make(map[string]interface{})
			propsMap["vid"] = vid

			// Add properties, skipping _vid/vid column
			for i, colName := range columnNames {
				colStr := strings.ToLower(string(colName))
				if colStr == "_vid" || colStr == "vid" {
					continue
				}
				// Strip tag prefix if present (e.g., "genre.name" -> "name")
				propName := colStr
				if idx := strings.Index(colStr, "."); idx > 0 {
					propName = colStr[idx+1:]
				}
				if i < len(values) {
					propsMap[propName] = s.valueToInterface(values[i])
				}
			}
			batch = append(batch, propsMap)

			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}

		newParts := resp.GetCursors()
		if newParts == nil || !hasRemainingCursors(newParts) {
			if err := flushBatch(); err != nil {
				return err
			}
			break
		}
		parts = newParts
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (s *StorageScanner) ScanEdges(edgeName string, batchSize int, handler func([]map[string]interface{}) error) error {
	edgeType, err := s.getEdgeID(edgeName)
	if err != nil {
		return fmt.Errorf("failed to get edge ID: %w", err)
	}
	logs.Info("[DEBUG] ScanEdges edgeName: %v edgeType: %v", edgeName, edgeType)

	parts := make(map[nebula.PartitionID]*storage.ScanCursor)
	for _, pid := range s.partIDs {
		parts[pid] = nil
	}

	batch := make([]map[string]interface{}, 0, batchSize)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := handler(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	sessionIDVal := s.sessionID
	common := &storage.RequestCommon{
		SessionID: &sessionIDVal,
	}

	returnColumns := []*storage.EdgeProp{
		{Type: edgeType, Props: [][]byte{[]byte("_src"), []byte("_type"), []byte("_rank"), []byte("_dst")}},
	}

	for {
		req := &storage.ScanEdgeRequest{
			SpaceID:                s.spaceID,
			Parts:                  parts,
			ReturnColumns:          returnColumns,
			Limit:                  int64(batchSize),
			OnlyLatestVersion:      true,
			EnableReadFromFollower: true,
			Common:                 common,
		}

		resp, err := s.client.ScanEdge(req)
		if err != nil {
			if isTimeoutError(err) && len(batch) > 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("scan edge failed: %w", err)
		}

		failedParts := resp.GetResult_().GetFailedParts()
		for _, part := range failedParts {
			if part.GetCode() == nebula.ErrorCode_E_PART_NOT_FOUND {
				continue
			}
			if part.GetCode() != nebula.ErrorCode_SUCCEEDED {
				return fmt.Errorf("scan failed on partition %d: error code %d", part.GetPartID(), part.GetCode())
			}
		}

		edgeData := resp.GetProps()
		if edgeData == nil || len(edgeData.GetRows()) == 0 {
			if err := flushBatch(); err != nil {
				return err
			}
			break
		}
		// xx._src, xx._type, xx._rank, xx._dst
		//columnNames := edgeData.GetColumnNames()
		logs.Info(fmt.Printf("edge column names : %v\n", edgeData.GetColumnNames()))
		for _, row := range edgeData.GetRows() {
			values := row.GetValues()
			//fmt.Printf("edged values: %v", values)
			if len(values) < 3 {
				continue
			}

			srcID := s.valueToString(values[0])
			var rank int64
			if values[2] != nil && values[2].IVal != nil {
				rank = *values[2].IVal
			}
			dstID := s.valueToString(values[3])
			propsMap := make(map[string]interface{})
			propsMap["_src"] = srcID
			propsMap["_dst"] = dstID
			propsMap["_rank"] = rank

			// for i := 3; i < len(columnNames)+3 && i < len(values); i++ {
			// 	propsMap[string(columnNames[i-3])] = s.valueToInterface(values[i])
			// }

			batch = append(batch, propsMap)

			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}

		newParts := resp.GetCursors()
		fmt.Printf("[debug] edge newparts : %v\n", newParts)
		if newParts == nil || !hasRemainingCursors(newParts) {
			if err := flushBatch(); err != nil {
				return err
			}
			break
		}
		parts = newParts
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (s *StorageScanner) valueToString(val *nebula.Value) string {
	if val == nil {
		return ""
	}
	if val.SVal != nil {
		return string(val.SVal)
	}
	if val.IVal != nil {
		return fmt.Sprintf("%d", *val.IVal)
	}
	if val.FVal != nil {
		return fmt.Sprintf("%f", *val.FVal)
	}
	return ""
}

func (s *StorageScanner) valueToInterface(val *nebula.Value) interface{} {
	if val == nil {
		return nil
	}
	if val.SVal != nil {
		return string(val.SVal)
	}
	if val.IVal != nil {
		return *val.IVal
	}
	if val.FVal != nil {
		return *val.FVal
	}
	if val.BVal != nil {
		return *val.BVal
	}
	return nil
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "deadline") ||
		strings.Contains(errStr, "i/o timeout")
}

func hasRemainingCursors(cursors map[nebula.PartitionID]*storage.ScanCursor) bool {
	if cursors == nil {
		return false
	}
	for _, cursor := range cursors {
		if cursor != nil && len(cursor.GetNextCursor()) > 0 {
			return true
		}
	}
	return false
}
