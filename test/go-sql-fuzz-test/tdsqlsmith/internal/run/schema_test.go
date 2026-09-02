package run

import (
	"testing"

	"tdsqlsmith/internal/catalog"
)

func TestSchemaFromPrepared(t *testing.T) {
	in := &catalog.Prepared{
		Database: "x",
		Tables: []catalog.Table{
			{
				Name: "t1",
				Columns: []catalog.Column{
					{Name: "ts", Type: "timestamp"},
					{Name: "v", Type: "double"},
				},
			},
			{
				Name: "t2",
				Columns: []catalog.Column{
					{Name: "id", Type: "int"},
				},
			},
		},
	}
	out := schemaFromPrepared(in)
	if len(out.Tables) != 2 {
		t.Fatalf("unexpected table count: %d", len(out.Tables))
	}
	if out.Tables[0].Name != "t1" || len(out.Tables[0].Columns) != 2 {
		t.Fatalf("unexpected first table: %#v", out.Tables[0])
	}
}
