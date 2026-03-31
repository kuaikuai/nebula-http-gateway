package es

import (
	"testing"
)

func TestGenDocID(t *testing.T) {
	tests := []struct {
		name     string
		vid      string
		src      string
		dst      string
		rank     int64
		expected int // 期望长度
	}{
		{"vertex with vid", "tom", "", "", 0, 64},
		{"vertex with empty vid", "", "", "", 0, 64},
		{"edge with src and dst", "", "user1", "user2", 0, 64},
		{"edge with rank", "", "src", "dst", 5, 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := genDocID(tt.vid, tt.src, tt.dst, tt.rank)
			if len(result) != tt.expected {
				t.Errorf("genDocID() length = %d, want %d", len(result), tt.expected)
			}
		})
	}
}

func TestGenDocID_Consistency(t *testing.T) {
	// 相同输入应该产生相同的 docID
	docID1 := genDocID("tom", "", "", 0)
	docID2 := genDocID("tom", "", "", 0)

	if docID1 != docID2 {
		t.Errorf("Same input should produce same docID: got %s and %s", docID1, docID2)
	}
}

func TestGenDocID_DifferentVID(t *testing.T) {
	// 不同 vid 应该产生不同的 docID
	docID1 := genDocID("alice", "", "", 0)
	docID2 := genDocID("bob", "", "", 0)

	if docID1 == docID2 {
		t.Error("Different vid should produce different docID")
	}
}

func TestGenDocID_VertexEdgeDifference(t *testing.T) {
	// Vertex 和 Edge 的 docID 格式不同
	vertexDocID := genDocID("person1", "", "", 0)
	edgeDocID := genDocID("", "person1", "person2", 0)

	if vertexDocID == edgeDocID {
		t.Error("Vertex and Edge should produce different docID")
	}
}
