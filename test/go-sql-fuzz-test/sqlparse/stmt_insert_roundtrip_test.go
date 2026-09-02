package sqlparser

import (
	"reflect"
	"testing"
)

func TestInsertStatement_FormatRoundTrip(t *testing.T) {
	cases := []string{
		"insert into test.d_1 (`ts`, `current`, `voltage` , `status`) values (1627891234, 10.2, 219, 0.31)",
		"insert into test.d_1 using test.meters (`Groupid`,`location`) tags (1,'location1') (`ts`, `current`, `voltage` , `status`) values (1627891234, 10.2, 219, 0.31), (1627891294, 10.5, 220, 0.29) test.d_2 using test.meters2 (`location`,`Groupid`) tags('location2',2) values(1627891235, 11.2, 221, 0.32)(1627891236, 11.3, 223, 0.42)",
		"insert into test.d_1  (`ts`, `current`, `voltage` , `status`) values (now, 10.2, 219, 0.31) (today, 10.2, 219, 0.31) test.d_2 values(now(),10.3,220,0.33)(today(),11.3,221,0.31)",
	}
	for _, sql := range cases {
		stmt1, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
		ins1, ok := stmt1.(InsertStatement)
		if !ok {
			t.Fatalf("unexpected type for %q: %T", sql, stmt1)
		}
		tb := newTB()
		ins1.Format(tb)
		formatted := tb.String()
		stmt2, err := Parse(formatted)
		if err != nil {
			t.Fatalf("reparse formatted failed sql=%q formatted=%q err=%v", sql, formatted, err)
		}
		ins2, ok := stmt2.(InsertStatement)
		if !ok {
			t.Fatalf("unexpected reparse type for %q: %T", sql, stmt2)
		}
		if !reflect.DeepEqual(ins1, ins2) {
			t.Fatalf("insert mismatch sql=%q formatted=%q\nstmt1=%#v\nstmt2=%#v", sql, formatted, ins1, ins2)
		}
	}
}
