package diagnose

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

var snakeCaseJSONName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// TestReportJSONFieldsHaveExplicitSnakeCaseTags locks the wire contract for
// library consumers. A tag written after // is only comment text, so encoding/json
// silently falls back to the exported Go field name (RowsRead, Command, ...).
func TestReportJSONFieldsHaveExplicitSnakeCaseTags(t *testing.T) {
	reportTypes := []reflect.Type{
		reflect.TypeOf(ReportHeader{}),
		reflect.TypeOf(ReplicationQueueReport{}),
		reflect.TypeOf(TooManyPartsReport{}),
		reflect.TypeOf(ReplicaStatusReport{}),
		reflect.TypeOf(MutationsStuckReport{}),
		reflect.TypeOf(QueueBacklogReport{}),
		reflect.TypeOf(DiskSpaceReport{}),
		reflect.TypeOf(DetachedPartsReport{}),
		reflect.TypeOf(ResourcePressureReport{}),
		reflect.TypeOf(SlowQueryReport{}),
		reflect.TypeOf(DistributedDDLReport{}),
	}

	packagePath := reflect.TypeOf(NodeResult{}).PkgPath()
	visited := make(map[reflect.Type]bool)
	for _, typ := range reportTypes {
		checkJSONTags(t, typ, packagePath, visited)
	}
}

func checkJSONTags(t *testing.T, typ reflect.Type, packagePath string, visited map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || typ.PkgPath() != packagePath || visited[typ] {
		return
	}
	visited[typ] = true

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		if !field.Anonymous {
			tag, ok := field.Tag.Lookup("json")
			if !ok {
				t.Errorf("%s.%s 缺少显式 json tag", typ.Name(), field.Name)
			} else {
				name := strings.Split(tag, ",")[0]
				if name != "-" && !snakeCaseJSONName.MatchString(name) {
					t.Errorf("%s.%s json tag = %q，不是 snake_case", typ.Name(), field.Name, tag)
				}
			}
		}
		checkJSONTags(t, field.Type, packagePath, visited)
	}
}

func TestNodeResultJSONUsesSnakeCaseKeys(t *testing.T) {
	out, err := json.Marshal(NodeResult{
		NodeID:      "node-a",
		ShardID:     "shard-1",
		Status:      NodeFailed,
		RowsRead:    3,
		RowsKept:    2,
		Truncated:   true,
		ErrCategory: ErrCatQueryFailed,
		ErrMessage:  "boom",
	})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, key := range []string{
		"node_id", "shard_id", "status", "rows_read", "rows_kept",
		"truncated", "err_category", "err_message", "started_at", "finished_at",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("JSON 缺少 key %q: %s", key, out)
		}
	}
	for _, oldKey := range []string{"RowsRead", "RowsKept", "Truncated", "ErrCategory", "ErrMessage"} {
		if _, ok := got[oldKey]; ok {
			t.Errorf("JSON 仍包含 Go 字段名 %q: %s", oldKey, out)
		}
	}
}
