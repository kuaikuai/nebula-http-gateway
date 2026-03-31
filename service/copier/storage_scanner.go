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
	//"github.com/vesoft-inc/nebula-http-gateway/ccore/nebula/gateway/pool"
)

type StorageScanner struct {
	nsid           string
	storageAddr    string
	client         *storage.GraphStorageServiceClient
	spaceID        nebula.GraphSpaceID
	spaceName      string
	partInfo       []PartitionInfo
	sessionID      int64
	storageClients map[string]*storage.GraphStorageServiceClient
}

func NewStorageScanner(nsid, spaceName string) (*StorageScanner, error) {
	scanner := &StorageScanner{
		nsid:           nsid,
		spaceName:      spaceName,
		storageClients: make(map[string]*storage.GraphStorageServiceClient, 0),
	}
	spaceID, err := scanner.getSpaceID(spaceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get space ID: %w", err)
	}
	scanner.spaceID = spaceID
	logs.Info("new scanner spaceName:%s spaceID: %v", spaceName, spaceID)
	// 获取分区信息（包括 Leader）
	partInfo, err := scanner.getPartitionInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get partition info: %w", err)
	}
	if len(partInfo) == 0 {
		return nil, fmt.Errorf("no partitions found")
	}
	scanner.partInfo = partInfo

	// 收集唯一的 Leader 地址
	uniqueAddrs := make(map[string]bool)
	for _, p := range partInfo {
		if p.Leader != "" {
			uniqueAddrs[p.Leader] = true
		}
	}

	// 为每个唯一地址创建 client
	for addr := range uniqueAddrs {
		logs.Info("Creating storage client for: %s", addr)
		client, err := createStorageClient(addr)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage client for %s: %w", addr, err)
		}
		scanner.storageClients[addr] = client
	}

	return scanner, nil
}

func (s *StorageScanner) Close() error {
	for _, client := range s.storageClients {
		if client != nil {
			client.Close()
		}
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

// PartitionInfo holds partition and its leader address
type PartitionInfo struct {
	PartID nebula.PartitionID
	Leader string // host:port
}

func (s *StorageScanner) getPartitionInfo() ([]PartitionInfo, error) {
	result, _, err := dao.Execute(s.nsid, fmt.Sprintf("USE %s; SHOW PARTS", s.spaceName), nil)
	if err != nil {
		return nil, err
	}

	var parts []PartitionInfo
	for _, row := range result.Tables {
		if partID, ok := row["Partition ID"].(int64); ok {
			var leader string
			if leaderVal, ok := row["Leader"]; ok {
				leader = fmt.Sprintf("%v", leaderVal)
			}
			parts = append(parts, PartitionInfo{
				PartID: nebula.PartitionID(partID),
				Leader: leader,
			})
		}
	}
	return parts, nil
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
	maxRetries := 10
	retryInterval := 5 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		explainGql := fmt.Sprintf("EXPLAIN format='row' MATCH (n:%s) RETURN n LIMIT 1", tagName)
		result, _, err := dao.Execute(s.nsid, explainGql, nil)
		if err != nil {
			lastErr = err
			logs.Warn("getTagID attempt %d/%d failed: %v", i+1, maxRetries, err)
			time.Sleep(retryInterval)
			continue
		}
		for _, row := range result.Tables {
			if opName, ok := row["name"].(string); ok {
				if opName == "ScanVertices" {
					if opInfo, ok := row["operator info"].(string); ok {
						if tagID, found := extractTagID(opInfo); found {
							return nebula.TagID(tagID), nil
						}
					}
				}
			}
		}
		lastErr = fmt.Errorf("tag ID not found for %s, result tables:%v", tagName, result.Tables)
		logs.Warn("getTagID attempt %d/%d: %v", i+1, maxRetries, lastErr)
		time.Sleep(retryInterval)
	}

	return 0, fmt.Errorf("tag ID not found for %s after %d attempts: %w", tagName, maxRetries, lastErr)
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
	maxRetries := 10
	retryInterval := 5 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		explainGql := fmt.Sprintf("EXPLAIN format='row' MATCH (a)-[r:%s]->(b) RETURN r LIMIT 1", edgeName)
		result, _, err := dao.Execute(s.nsid, explainGql, nil)
		if err != nil {
			lastErr = err
			logs.Warn("getEdgeID attempt %d/%d failed: %v", i+1, maxRetries, err)
			time.Sleep(retryInterval)
			continue
		}
		for _, row := range result.Tables {
			if opName, ok := row["name"].(string); ok {
				if opName == "Traverse" {
					if opInfo, ok := row["operator info"].(string); ok {
						if edgeTypeID, found := extractEdgeTypeID(opInfo); found {
							return nebula.EdgeType(edgeTypeID), nil
						}
					}
				}
			}
		}
		lastErr = fmt.Errorf("edge ID not found for %s, result tables:%v", edgeName, result.Tables)
		logs.Warn("getEdgeID attempt %d/%d: %v", i+1, maxRetries, lastErr)
		time.Sleep(retryInterval)
	}

	return 0, fmt.Errorf("edge ID not found for %s after %d attempts: %w", edgeName, maxRetries, lastErr)
}

// getEdgeProps 获取 edge 的所有属性名
func (s *StorageScanner) getEdgeProps(edgeName string) ([]string, error) {
	descGql := fmt.Sprintf("USE %s; DESCRIBE EDGE %s", s.spaceName, edgeName)
	descResult, _, err := dao.Execute(s.nsid, descGql, nil)
	if err != nil {
		return nil, fmt.Errorf("describe edge failed: %w", err)
	}

	var props []string
	for _, row := range descResult.Tables {
		if name, ok := row["Field"].(string); ok {
			if name == "_src" || name == "_dst" || name == "_rank" || name == "_type" {
				continue
			}
			props = append(props, name)
		}
	}
	return props, nil
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

func createStorageClient(addr string) (*storage.GraphStorageServiceClient, error) {
	parts := strings.Split(strings.TrimSpace(addr), ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid storage address: %s", addr)
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

type partState struct {
	cursor *storage.ScanCursor
	done   bool
	leader string
}

// initPartitions 初始化分区状态
func (s *StorageScanner) initPartitions() map[nebula.PartitionID]*partState {
	partStates := make(map[nebula.PartitionID]*partState)
	for _, p := range s.partInfo {
		partStates[p.PartID] = &partState{leader: p.Leader}
	}
	return partStates
}

func (s *StorageScanner) ScanVertices(tagName string, batchSize int, handler func([]map[string]interface{}) error) error {
	logs.Info("scanVertices spaceName: %s, spaceID: %v, tagName: %v", s.spaceName, s.spaceID, tagName)
	tagID, err := s.getTagID(tagName)
	if err != nil {
		return fmt.Errorf("failed to get tag ID: %w", err)
	}
	logs.Info("ScanVertices tagID: %v", tagID)
	// 初始化分区状态
	partStates := s.initPartitions()

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
		hasData := false

		// 逐分区扫描
		for pid, state := range partStates {
			if state.done {
				continue
			}

			// 获取该分区对应的 client
			client, ok := s.storageClients[state.leader]
			if !ok {
				logs.Error("no client for leader: %s", state.leader)
				continue
			}

			// 构建单分区请求
			reqParts := map[nebula.PartitionID]*storage.ScanCursor{pid: state.cursor}

			req := &storage.ScanVertexRequest{
				SpaceID:                s.spaceID,
				Parts:                  reqParts,
				ReturnColumns:          returnColumns,
				Limit:                  int64(batchSize),
				OnlyLatestVersion:      true,
				EnableReadFromFollower: true,
				Common:                 common,
			}

			resp, err := client.ScanVertex(req)
			if err != nil {
				if isTimeoutError(err) && len(batch) > 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				logs.Error("scan vertex failed for partition %d: %v", pid, err)
				continue
			}

			failedParts := resp.GetResult_().GetFailedParts()
			for _, part := range failedParts {
				// if part.GetCode() == nebula.ErrorCode_E_PART_NOT_FOUND {
				// 	continue
				// }
				if part.GetCode() != nebula.ErrorCode_SUCCEEDED {
					logs.Error("scan failed on partition %d: error code %d", part.GetPartID(), part.GetCode())
					continue
				}
			}

			vertexData := resp.GetProps()
			if vertexData == nil || len(vertexData.GetRows()) == 0 {
				state.done = true
				continue
			}

			hasData = true

			columnNames := vertexData.GetColumnNames()
			//logs.Info("[DEBUG] ScanVertices partition %d columnNames: %v", pid, columnNames)

			for _, row := range vertexData.GetRows() {
				values := row.GetValues()
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
					vid = s.valueToString(values[vidColumnIndex])
				} else {
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

			// 更新分区状态
			cursors := resp.GetCursors()
			if cursor, ok := cursors[pid]; ok && len(cursor.GetNextCursor()) > 0 {
				state.cursor = cursor
			} else {
				state.done = true
			}
		}

		// 检查是否全部完成
		if !hasData || allPartsDone(partStates) {
			if err := flushBatch(); err != nil {
				return err
			}
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

func (s *StorageScanner) ScanEdges(edgeName string, batchSize int, handler func([]map[string]interface{}) error) error {
	logs.Info("ScanEdges spaceName: %s, spaceID: %v edgeName: %s",
		s.spaceName, s.spaceID, edgeName)
	edgeType, err := s.getEdgeID(edgeName)
	if err != nil {
		return fmt.Errorf("failed to get edge ID: %w", err)
	}
	logs.Info("edgeName %v, edgeType: %v", edgeName, edgeType)

	// 获取 edge 的所有属性
	edgeProps, err := s.getEdgeProps(edgeName)
	if err != nil {
		return fmt.Errorf("failed to get edge props: %w", err)
	}

	// 初始化分区状态
	partStates := s.initPartitions()

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

	// 构建返回列：系统属性 + 自定义属性
	props := [][]byte{[]byte("_src"), []byte("_type"), []byte("_rank"), []byte("_dst")}
	for _, prop := range edgeProps {
		props = append(props, []byte(prop))
	}

	returnColumns := []*storage.EdgeProp{
		{Type: edgeType, Props: props},
	}

	for {
		hasData := false

		// 逐分区扫描
		for pid, state := range partStates {
			if state.done {
				continue
			}

			// 获取该分区对应的 client
			client, ok := s.storageClients[state.leader]
			if !ok {
				logs.Error("no client for leader: %s", state.leader)
				continue
			}

			// 构建单分区请求
			reqParts := map[nebula.PartitionID]*storage.ScanCursor{pid: state.cursor}

			req := &storage.ScanEdgeRequest{
				SpaceID:                s.spaceID,
				Parts:                  reqParts,
				ReturnColumns:          returnColumns,
				Limit:                  int64(batchSize),
				OnlyLatestVersion:      true,
				EnableReadFromFollower: true,
				Common:                 common,
			}

			resp, err := client.ScanEdge(req)
			if err != nil {
				if isTimeoutError(err) && len(batch) > 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				logs.Error("scan edge failed for partition %d: %v", pid, err)
				continue
			}

			failedParts := resp.GetResult_().GetFailedParts()
			for _, part := range failedParts {
				// if part.GetCode() == nebula.ErrorCode_E_PART_NOT_FOUND {
				// 	continue
				// }
				if part.GetCode() != nebula.ErrorCode_SUCCEEDED {
					logs.Error("scan failed on partition %d, spaceID %d: rror code %d", part.GetPartID(), s.spaceID, part.GetCode())
					continue
				}
			}

			edgeData := resp.GetProps()
			if edgeData == nil || len(edgeData.GetRows()) == 0 {
				state.done = true
				continue
			}

			hasData = true
			columnNames := edgeData.GetColumnNames()

			for _, row := range edgeData.GetRows() {
				values := row.GetValues()
				if len(values) < 4 {
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

				// 添加自定义属性
				for i := 4; i < len(values) && i-4 < len(columnNames)-4; i++ {
					propName := string(columnNames[i])
					// 跳过系统属性前缀
					if idx := strings.Index(propName, "."); idx > 0 {
						propName = propName[idx+1:]
					}
					propsMap[propName] = s.valueToInterface(values[i])
				}

				batch = append(batch, propsMap)

				if len(batch) >= batchSize {
					if err := flushBatch(); err != nil {
						return err
					}
				}
			}

			// 更新分区状态
			cursors := resp.GetCursors()
			if cursor, ok := cursors[pid]; ok && len(cursor.GetNextCursor()) > 0 {
				state.cursor = cursor
			} else {
				state.done = true
			}
		}

		// 检查是否全部完成
		if !hasData || allPartsDone(partStates) {
			if err := flushBatch(); err != nil {
				return err
			}
			break
		}

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

// allPartsDone 检查所有分区是否完成
func allPartsDone(partStates map[nebula.PartitionID]*partState) bool {
	for _, state := range partStates {
		if !state.done {
			return false
		}
	}
	return true
}
