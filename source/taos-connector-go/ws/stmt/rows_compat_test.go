package stmt

import "testing"

func TestRowsColumnTypeLengthFromLegacyMetadata(t *testing.T) {
	rows := NewRows(nil, nil, &UseResultResp{
		FieldsLengths: []int64{8, 16},
	}, nil)

	length, ok := rows.ColumnTypeLength(1)
	if !ok {
		t.Fatal("expected ColumnTypeLength ok=true for valid legacy metadata index")
	}
	if length != 16 {
		t.Fatalf("unexpected length %d", length)
	}

	_, ok = rows.ColumnTypeLength(2)
	if ok {
		t.Fatal("expected ColumnTypeLength ok=false for out-of-range index")
	}
}
